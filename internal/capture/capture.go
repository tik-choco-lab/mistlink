//go:build windows

package capture

import (
	"fmt"
	"image"
	"strings"
	"syscall"
	"unsafe"

	"github.com/kbinani/screenshot"
)

var (
	user32                       = syscall.NewLazyDLL("user32.dll")
	procEnumWindows              = user32.NewProc("EnumWindows")
	procGetWindowText            = user32.NewProc("GetWindowTextW")
	procIsWindowVisible          = user32.NewProc("IsWindowVisible")
	procIsIconic                 = user32.NewProc("IsIconic")
	procGetWindowRect            = user32.NewProc("GetWindowRect")
	procGetWindowThreadProcessId = user32.NewProc("GetWindowThreadProcessId")
	procSetProcessDPIAware       = user32.NewProc("SetProcessDPIAware")
	procPrintWindow              = user32.NewProc("PrintWindow")
	procGetWindowDC              = user32.NewProc("GetWindowDC")
	procReleaseDC                = user32.NewProc("ReleaseDC")

	gdi32                      = syscall.NewLazyDLL("gdi32.dll")
	procCreateCompatibleDC     = gdi32.NewProc("CreateCompatibleDC")
	procCreateCompatibleBitmap = gdi32.NewProc("CreateCompatibleBitmap")
	procSelectObject           = gdi32.NewProc("SelectObject")
	procDeleteDC               = gdi32.NewProc("DeleteDC")
	procDeleteObject           = gdi32.NewProc("DeleteObject")
	procGetDIBits              = gdi32.NewProc("GetDIBits")
)

const (
	PW_RENDERFULLCONTENT = 0x00000002
	DIB_RGB_COLORS       = 0
	BI_RGB               = 0
)

type RECT struct {
	Left, Top, Right, Bottom int32
}

type BITMAPINFOHEADER struct {
	Size          uint32
	Width         int32
	Height        int32
	Planes        uint16
	BitCount      uint16
	Compression   uint32
	SizeImage     uint32
	XPelsPerMeter int32
	YPelsPerMeter int32
	ClrUsed       uint32
	ClrImportant  uint32
}

type BITMAPINFO struct {
	Header BITMAPINFOHEADER
}

type WindowInfo struct {
	Handle uintptr
	Title  string
}

func Init() {
	procSetProcessDPIAware.Call()
}

func ListWindows() ([]WindowInfo, error) {
	var windows []WindowInfo
	cb := syscall.NewCallback(func(h syscall.Handle, lparam uintptr) uintptr {
		visible, _, _ := procIsWindowVisible.Call(uintptr(h))
		if visible == 0 {
			return 1
		}

		b := make([]uint16, 200)
		ret, _, _ := procGetWindowText.Call(uintptr(h), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
		if ret == 0 {
			return 1
		}

		title := syscall.UTF16ToString(b)
		if title == "" {
			return 1
		}

		windows = append(windows, WindowInfo{
			Handle: uintptr(h),
			Title:  title,
		})
		return 1
	})

	procEnumWindows.Call(cb, 0)
	return windows, nil
}

func CaptureEntireScreen() (image.Image, error) {
	n := screenshot.NumActiveDisplays()
	if n <= 0 {
		return nil, fmt.Errorf("no active displays found")
	}
	bounds := screenshot.GetDisplayBounds(0)
	return screenshot.CaptureRect(bounds)
}

func CaptureWindow(hwnd uintptr) (image.Image, error) {
	var rect RECT
	ret, _, _ := procGetWindowRect.Call(hwnd, uintptr(unsafe.Pointer(&rect)))
	if ret == 0 {
		return nil, fmt.Errorf("failed to get window rect")
	}

	width := int(rect.Right - rect.Left)
	height := int(rect.Bottom - rect.Top)
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("invalid window dimensions")
	}

	hwndDC, _, _ := procGetWindowDC.Call(hwnd)
	defer procReleaseDC.Call(hwnd, hwndDC)

	mfcDC, _, _ := procCreateCompatibleDC.Call(hwndDC)
	defer procDeleteDC.Call(mfcDC)

	saveBitMap, _, _ := procCreateCompatibleBitmap.Call(hwndDC, uintptr(width), uintptr(height))
	defer procDeleteObject.Call(saveBitMap)

	procSelectObject.Call(mfcDC, saveBitMap)

	ret, _, _ = procPrintWindow.Call(hwnd, mfcDC, uintptr(PW_RENDERFULLCONTENT))
	if ret == 0 {
		procPrintWindow.Call(hwnd, mfcDC, 0)
	}

	var bi BITMAPINFO
	bi.Header.Size = uint32(unsafe.Sizeof(bi.Header))
	bi.Header.Width = int32(width)
	bi.Header.Height = int32(-height)
	bi.Header.Planes = 1
	bi.Header.BitCount = 32
	bi.Header.Compression = BI_RGB

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	ret, _, _ = procGetDIBits.Call(hwndDC, saveBitMap, 0, uintptr(height), uintptr(unsafe.Pointer(&img.Pix[0])), uintptr(unsafe.Pointer(&bi)), DIB_RGB_COLORS)
	if ret == 0 {
		return nil, fmt.Errorf("failed to get bitmap bits")
	}

	for i := 0; i < len(img.Pix); i += 4 {
		b, r := img.Pix[i], img.Pix[i+2]
		img.Pix[i], img.Pix[i+2] = r, b
		img.Pix[i+3] = 255
	}

	return img, nil
}

func FindWindowByTitle(title string) uintptr {
	var hwnd uintptr
	targetLower := strings.ToLower(title)

	cb := syscall.NewCallback(func(h syscall.Handle, lparam uintptr) uintptr {
		b := make([]uint16, 200)
		ret, _, _ := procGetWindowText.Call(uintptr(h), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
		if ret != 0 {
			t := strings.ToLower(syscall.UTF16ToString(b))
			if strings.Contains(t, targetLower) {
				visible, _, _ := procIsWindowVisible.Call(uintptr(h))
				if visible != 0 {
					hwnd = uintptr(h)
					return 0
				}
			}
		}
		return 1
	})
	procEnumWindows.Call(cb, 0)
	return hwnd
}
