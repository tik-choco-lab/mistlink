package receiver

import (
	"fmt"

	"github.com/tik-choco-lab/mistlink/internal/logger"
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
	b.mu.Lock()
	defer b.mu.Unlock()

	switch nalType {
	case 7: // SPS
		logger.Debugf("RTSP", "Received SPS NAL: %d bytes", len(payload))
		b.sps = append([]byte{}, payload...)
	case 8: // PPS
		logger.Debugf("RTSP", "Received PPS NAL: %d bytes", len(payload))
		b.pps = append([]byte{}, payload...)
	case 24: // STAP-A
		pos := 1
		for pos+2 <= len(payload) {
			size := int(payload[pos])<<8 | int(payload[pos+1])
			pos += 2
			if pos+size > len(payload) {
				break
			}
			unit := payload[pos : pos+size]
			if len(unit) > 0 {
				nt := unit[0] & 0x1F
				if nt == 7 && len(b.sps) == 0 {
					b.sps = append([]byte{}, unit...)
				} else if nt == 8 && len(b.pps) == 0 {
					b.pps = append([]byte{}, unit...)
				}
			}
			pos += size
		}
	case 28: // FU-A
		if len(payload) > 1 && (payload[1]&0x80) != 0 {
			orig := payload[1] & 0x1F
			if orig == 7 && len(b.sps) == 0 {
				unit := append([]byte{(payload[0] & 0xE0) | orig}, payload[2:]...)
				b.sps = append([]byte{}, unit...)
			} else if orig == 8 && len(b.pps) == 0 {
				unit := append([]byte{(payload[0] & 0xE0) | orig}, payload[2:]...)
				b.pps = append([]byte{}, unit...)
			}
		}
	}

	_ = b.tryStartLocked()
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
		srv, err := rtspserver.StartServer(b.rtspPort, b.audioCodec)
		if err != nil {
			return fmt.Errorf("RTSP server start error: %w", err)
		}
		b.server = srv
	}

	if err := b.server.InitStream(b.sps, b.pps); err != nil {
		return fmt.Errorf("RTSP stream init error: %w", err)
	}
	b.started = true
	logger.Debugf("RTSP", "RTSP Server ready: rtsp://localhost:%d/stream (SPS: %d bytes, PPS: %d bytes)", b.rtspPort, len(b.sps), len(b.pps))
	return nil
}
