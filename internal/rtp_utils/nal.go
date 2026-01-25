package rtp_utils

import "fmt"

const (
	PayloadTypeH264 = 96
	PayloadTypeOpus = 111
	PayloadTypeAAC  = 112

	VideoSSRC = 0x12345678
	AudioSSRC = 0x87654321

	MaxTimestampDelta = 90000

	LogIntervalPackets      = 100
	DummyTimestampIncrement = 9000

	NALTypeNonIDR = 1
	NALTypeIDR    = 5
	NALTypeSEI    = 6
	NALTypeSPS    = 7
	NALTypePPS    = 8
	NALTypeSTAPA  = 24
	NALTypeFUA    = 28
	NALMask       = 0x1F
	FUStartMask   = 0x80
	FUEndMask     = 0x40
)

func GetNALType(payload []byte) byte {
	if len(payload) == 0 {
		return 0
	}
	return payload[0] & NALMask
}

func GetNALTypeName(nalType byte) string {
	switch nalType {
	case NALTypeNonIDR:
		return "Non-IDR"
	case NALTypeIDR:
		return "IDR"
	case NALTypeSEI:
		return "SEI"
	case NALTypeSPS:
		return "SPS"
	case NALTypePPS:
		return "PPS"
	case NALTypeSTAPA:
		return "STAP-A"
	case NALTypeFUA:
		return "FU-A"
	default:
		return fmt.Sprintf("Unknown(%d)", nalType)
	}
}
