package stream

import (
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)

type TrackBroadcaster struct {
	track      *webrtc.TrackRemote
	rtcpWriter func([]rtcp.Packet) error
	bridge     *receiver.RTPBridge

	receivers map[string]*webrtc.TrackLocalStaticRTP
	mu        sync.RWMutex

	sps []byte
	pps []byte

	closed chan struct{}
}

func NewTrackBroadcaster(
	track *webrtc.TrackRemote,
	rtcpWriter func([]rtcp.Packet) error,
	bridge *receiver.RTPBridge,
) *TrackBroadcaster {
	return &TrackBroadcaster{
		track:      track,
		rtcpWriter: rtcpWriter,
		bridge:     bridge,
		receivers:  make(map[string]*webrtc.TrackLocalStaticRTP),
		closed:     make(chan struct{}),
	}
}

func (b *TrackBroadcaster) AddReceiver(id string, localTrack *webrtc.TrackLocalStaticRTP) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.receivers[id] = localTrack

	if len(b.sps) > 0 {
		if _, err := localTrack.Write(b.sps); err != nil {
			logger.Errorf("stream", "Error sending cached SPS to %s: %v", id, err)
		}
	}
	if len(b.pps) > 0 {
		if _, err := localTrack.Write(b.pps); err != nil {
			logger.Errorf("stream", "Error sending cached PPS to %s: %v", id, err)
		}
	}

	if b.rtcpWriter != nil {
		go receiver.SendPLI(b.rtcpWriter, b.track.SSRC())
	}
}

func (b *TrackBroadcaster) RemoveReceiver(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.receivers, id)
}

func (b *TrackBroadcaster) Start() {
	go b.run()
}

func (b *TrackBroadcaster) run() {
	isVideo := strings.EqualFold(b.track.Codec().MimeType, webrtc.MimeTypeH264)
	isAudio := strings.EqualFold(b.track.Codec().MimeType, webrtc.MimeTypeOpus)

	if !isVideo && !isAudio {
		return
	}

	ssrc := uint32(b.track.SSRC())
	if b.bridge != nil {
		b.bridge.TrackStarted(ssrc, b.track.Codec().MimeType)
		defer b.bridge.TrackStopped(ssrc)
	}

	logger.Debugf("stream", "Broadcaster started for track: %s (SSRC: %d)", b.track.Codec().MimeType, b.track.SSRC())
	stats := newTrackStats()

	lastPLITime := time.Now()

	for {
		select {
		case <-b.closed:
			return
		default:
		}

		b.track.SetReadDeadline(time.Now().Add(5 * time.Second))
		pkt, _, err := b.track.ReadRTP()
		if err != nil {
			logger.Errorf("stream", "Broadcaster read error: %v", err)
			return
		}

		if isVideo {
			missing := stats.checkSequenceGap(pkt.SequenceNumber)
			if len(missing) > 0 {
				receiver.SendNACK(b.rtcpWriter, b.track.SSRC(), missing)
			}

			bridgeStarted := false
			if b.bridge != nil {
				bridgeStarted = b.bridge.IsStarted()
			}

			if !bridgeStarted && time.Since(lastPLITime) > 5*time.Second {
				receiver.SendPLI(b.rtcpWriter, b.track.SSRC())
				lastPLITime = time.Now()
			}

			b.extractSPSPPS(pkt)

			if b.bridge != nil {
				receiver.ProcessVideoPacket(pkt, b.bridge)
			}

			pkt.PayloadType = rtp_utils.PayloadTypeH264
		} else if isAudio {
			pkt.PayloadType = rtp_utils.PayloadTypeOpus
		}


		if b.bridge != nil {
			b.bridge.WriteRTP(pkt)
		}

		if len(b.receivers) > 0 {
			buf, err := pkt.Marshal()
			if err != nil {
				logger.Errorf("stream", "Packet marshal error: %v", err)
			} else {
				b.mu.RLock()
				for id, localTrack := range b.receivers {
					if _, err := localTrack.Write(buf); err != nil {
						logger.Errorf("stream", "Error writing to receiver %s: %v", id, err)
					}
				}
				b.mu.RUnlock()
			}
		}

		stats.packetCount++
		stats.logIfTime(isVideo)
	}
}

func (b *TrackBroadcaster) extractSPSPPS(pkt *rtp.Packet) {
	payload := pkt.Payload
	if len(payload) == 0 {
		return
	}

	nalType := payload[0] & 0x1F

	if nalType == rtp_utils.NALTypeSTAPA {
		pos := 1
		for pos+2 <= len(payload) {
			size := int(payload[pos])<<8 | int(payload[pos+1])
			pos += 2
			if pos+size > len(payload) {
				break
			}
			unit := payload[pos : pos+size]
			if len(unit) > 0 {
				unitType := unit[0] & 0x1F
				if unitType == rtp_utils.NALTypeSPS {
					b.sps = append([]byte(nil), unit...)
				} else if unitType == rtp_utils.NALTypePPS {
					b.pps = append([]byte(nil), unit...)
				}
			}
			pos += size
		}
	} else if nalType == rtp_utils.NALTypeSPS {
		b.sps = append([]byte(nil), payload...)
	} else if nalType == rtp_utils.NALTypePPS {
		b.pps = append([]byte(nil), payload...)
	}
}

type trackStats struct {
	lastSeq     uint16
	initialized bool
	packetCount uint64
	lastLogTime time.Time
}

func newTrackStats() *trackStats {
	return &trackStats{
		lastLogTime: time.Now(),
	}
}

func (s *trackStats) checkSequenceGap(seq uint16) []uint16 {
	if !s.initialized {
		s.lastSeq = seq
		s.initialized = true
		return nil
	}

	diff := seq - s.lastSeq
	if diff <= 1 {
		s.lastSeq = seq
		return nil
	}

	var missing []uint16
	if diff > 100 {
		s.lastSeq = seq
		return nil
	}

	for i := uint16(1); i < diff; i++ {
		missing = append(missing, s.lastSeq+i)
	}
	s.lastSeq = seq
	return missing
}

func (s *trackStats) logIfTime(isVideo bool) {
	if time.Since(s.lastLogTime) > 10*time.Second {
		s.lastLogTime = time.Now()
	}
}
