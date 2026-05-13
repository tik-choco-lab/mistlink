//go:build !windows

package capture

import (
	"fmt"
	"image"
)

type WindowInfo struct {
	Handle uintptr
	Title  string
}

func Init() {}

func ListWindows() ([]WindowInfo, error) {
	return nil, fmt.Errorf("window capture is only supported on Windows")
}

func CaptureEntireScreen() (image.Image, error) {
	return nil, fmt.Errorf("screen capture is only supported on Windows")
}

func CaptureWindow(hwnd uintptr) (image.Image, error) {
	return nil, fmt.Errorf("window capture is only supported on Windows")
}

func FindWindowByTitle(title string) uintptr {
	return 0
}
