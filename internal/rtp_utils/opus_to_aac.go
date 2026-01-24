package rtp_utils

/*
#cgo pkg-config: opus fdk-aac
#include "converter.h"
*/
import "C"
import (
	"unsafe"

	"github.com/pion/rtp"
)

type OpusToAACConverter struct {
	ctx *C.TranscodeCtx
	buf []byte
}

func NewOpusToAACConverter() (*OpusToAACConverter, error) {
	return &OpusToAACConverter{
		ctx: C.init_transcoder(),
		buf: make([]byte, 4096),
	}, nil
}

func (c *OpusToAACConverter) Convert(pkt *rtp.Packet) ([]byte, error) {
	if len(pkt.Payload) == 0 {
		return nil, nil
	}

	inPtr := (*C.uchar)(unsafe.Pointer(&pkt.Payload[0]))
	inLen := C.int(len(pkt.Payload))
	outPtr := (*C.uchar)(unsafe.Pointer(&c.buf[0]))
	outMax := C.int(len(c.buf))
	outLen := C.transcode_frame(c.ctx, inPtr, inLen, outPtr, outMax)

	if outLen <= 0 {
		return nil, nil
	}

	res := make([]byte, int(outLen))
	copy(res, c.buf[:outLen])
	return res, nil
}

func (c *OpusToAACConverter) Close() {
	C.free_transcoder(c.ctx)
}
