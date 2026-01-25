package receiver

import (
	"fmt"

	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)

func (b *RTPBridge) SetSPSPPS(sps []byte, pps []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(sps) > 0 && len(b.sps) == 0 {
		b.sps = append([]byte{}, sps...)
	}
	if len(pps) > 0 && len(b.pps) == 0 {
		b.pps = append([]byte{}, pps...)
	}

	_ = b.tryStartLocked()
}

func (b *RTPBridge) ExtractSPSPPS(payload []byte, nalType byte) {
	if len(payload) == 0 {
		return
	}

	var foundSPS, foundPPS []byte

	switch nalType {
	case rtp_utils.NALTypeSPS:
		logger.Debugf("RTSP", "Received SPS NAL: %d bytes", len(payload))
		foundSPS = append([]byte{}, payload...)
	case rtp_utils.NALTypePPS:
		logger.Debugf("RTSP", "Received PPS NAL: %d bytes", len(payload))
		foundPPS = append([]byte{}, payload...)
	case rtp_utils.NALTypeSTAPA:
		pos := 1
		for pos+2 <= len(payload) {
			size := int(payload[pos])<<8 | int(payload[pos+1])
			pos += 2
			if pos+size > len(payload) {
				break
			}
			unit := payload[pos : pos+size]
			if len(unit) > 0 {
				nt := unit[0] & rtp_utils.NALMask
				if nt == rtp_utils.NALTypeSPS && len(foundSPS) == 0 {
					foundSPS = append([]byte{}, unit...)
				} else if nt == rtp_utils.NALTypePPS && len(foundPPS) == 0 {
					foundPPS = append([]byte{}, unit...)
				}
			}
			pos += size
		}
	case rtp_utils.NALTypeFUA:
		if len(payload) > 1 && (payload[1]&rtp_utils.FUStartMask) != 0 {
			orig := payload[1] & rtp_utils.NALMask
			if orig == rtp_utils.NALTypeSPS {
				unit := append([]byte{(payload[0] & 0xE0) | orig}, payload[2:]...)
				foundSPS = append([]byte{}, unit...)
			} else if orig == rtp_utils.NALTypePPS {
				unit := append([]byte{(payload[0] & 0xE0) | orig}, payload[2:]...)
				foundPPS = append([]byte{}, unit...)
			}
		}
	}

	if len(foundSPS) > 0 || len(foundPPS) > 0 {
		b.SetSPSPPS(foundSPS, foundPPS)
	}
}

func (b *RTPBridge) tryStartLocked() error {
	if b.started {
		return nil
	}
	if len(b.sps) == 0 {
		return nil
	}
	if len(b.pps) == 0 {
		return nil
	}

	logger.Debugf("RTSP", "Starting RTSP stream (SPS: %d bytes, PPS: %d bytes)", len(b.sps), len(b.pps))

	if b.server == nil {
		srv, actualPort, err := rtspserver.StartServer(b.rtspHost, b.rtspPort, b.audioCodec)
		if err != nil {
			return fmt.Errorf("RTSP server start error: %w", err)
		}
		b.server = srv
		b.rtspPort = actualPort
	}

	if err := b.server.InitStream(b.sps, b.pps); err != nil {
		return fmt.Errorf("RTSP stream init error: %w", err)
	}
	b.started = true
	logger.Debugf("RTSP", "RTSP Server ready: rtsp://localhost:%d/stream (SPS: %d bytes, PPS: %d bytes)", b.rtspPort, len(b.sps), len(b.pps))
	return nil
}
