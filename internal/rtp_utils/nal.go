package rtp_utils

import "fmt"

const (
	PayloadTypeH264 = 96
	PayloadTypeOpus = 111

	NALTypeNonIDR = 1
	NALTypeIDR    = 5
	NALTypeSEI    = 6
	NALTypeSPS    = 7
	NALTypePPS    = 8
	NALTypeSTAPA  = 24
	NALTypeFUA    = 28
)

func GetNALType(payload []byte) byte {
	if len(payload) == 0 {
		return 0
	}
	return payload[0] & 0x1F
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
