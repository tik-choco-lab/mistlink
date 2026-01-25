package stream

import (
	"io"
	"net"
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

const (
	broadcasterReadTimeout   = 5 * time.Second
	bridgeStartupCheckWindow = 15 * time.Second
)

type TrackBroadcaster struct {
	track      *webrtc.TrackRemote
	rtcpWriter func([]rtcp.Packet) error
	bridge     *receiver.RTPBridge

	receiversVar map[string]*receiverWorker
	mu           sync.RWMutex

	sps []byte
	pps []byte

	closed chan struct{}
}

type receiverWorker struct {
	track *webrtc.TrackLocalStaticRTP
	ch    chan *rtp.Packet
	id    string
}

func NewTrackBroadcaster(
	track *webrtc.TrackRemote,
	rtcpWriter func([]rtcp.Packet) error,
	bridge *receiver.RTPBridge,
) *TrackBroadcaster {
	return &TrackBroadcaster{
		track:        track,
		rtcpWriter: rtcpWriter,
		bridge:     bridge,
		receiversVar: make(map[string]*receiverWorker),
		closed:     make(chan struct{}),
	}
}

func (b *TrackBroadcaster) AddReceiver(id string, localTrack *webrtc.TrackLocalStaticRTP) {
	b.mu.Lock()
	defer b.mu.Unlock()

	worker := &receiverWorker{
		id:    id,
		track: localTrack,
		ch:    make(chan *rtp.Packet, 16), 
	}
	b.receiversVar[id] = worker
	go worker.start()

	sendCachedNAL := func(nal []byte) {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:     2,
				PayloadType: rtp_utils.PayloadTypeH264,
			},
			Payload: nal,
		}
		select {
		case worker.ch <- pkt:
		default:
			logger.Warnf("stream", "Dropping cached NAL for new receiver %s, channel full", id)
		}
	}

	if len(b.sps) > 0 {
		sendCachedNAL(b.sps)
	}
	if len(b.pps) > 0 {
		sendCachedNAL(b.pps)
	}

	if b.rtcpWriter != nil {
		go receiver.SendPLI(b.rtcpWriter, b.track.SSRC())
	}
}

func (b *TrackBroadcaster) RemoveReceiver(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if worker, ok := b.receiversVar[id]; ok {
		close(worker.ch) 
		delete(b.receiversVar, id)
	}
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
	var trackID int
	if b.bridge != nil {
		trackID = b.bridge.TrackStarted(ssrc, b.track.Codec().MimeType)
		defer b.bridge.TrackStopped(ssrc, trackID)
	}

	logger.Debugf("stream", "Broadcaster started for track: %s (SSRC: %d)", b.track.Codec().MimeType, b.track.SSRC())
	stats := newTrackStats()

	lastPLITime := time.Now()

	for {
		pkt, shouldStop := b.readNextPacket()
		if shouldStop {
			return
		}
		if pkt == nil {
			continue
		}

		b.processPacket(pkt, isVideo, isAudio, stats, &lastPLITime, trackID)

		if b.bridge != nil {
			b.bridge.WriteRTP(pkt, trackID)
		}

		b.broadcastToReceivers(pkt)
	}
}

func (b *TrackBroadcaster) readNextPacket() (*rtp.Packet, bool) {
	select {
	case <-b.closed:
		return nil, true
	default:
	}

	b.track.SetReadDeadline(time.Now().Add(broadcasterReadTimeout))
	pkt, _, err := b.track.ReadRTP()
	if err == nil {
		return pkt, false
	}

	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		logger.Warnf("stream", "Broadcaster read timeout (no data). Retrying... (SSRC: %d)", b.track.SSRC())
		return nil, true
	}

	if err == io.EOF {
		logger.Debugf("stream", "Broadcaster track returned EOF. Stopping (SSRC: %d)", b.track.SSRC())
		return nil, true
	} else {
		logger.Warnf("stream", "Broadcaster read error: %v. Stopping (SSRC: %d)", err, b.track.SSRC())
		return nil, true
	}
}

func (b *TrackBroadcaster) processPacket(
	pkt *rtp.Packet,
	isVideo, isAudio bool,
	stats *trackStats,
	lastPLITime *time.Time,
	trackID int,
) {
	if isVideo {
		missing := stats.checkSequenceGap(pkt.SequenceNumber)
		if len(missing) > 0 {
			receiver.SendNACK(b.rtcpWriter, b.track.SSRC(), missing)
		}

		bridgeStarted := false
		if b.bridge != nil {
			bridgeStarted = b.bridge.IsStarted()
		}

		if !bridgeStarted && time.Since(*lastPLITime) > bridgeStartupCheckWindow {
			receiver.SendPLI(b.rtcpWriter, b.track.SSRC())
			*lastPLITime = time.Now()
		}

		b.extractSPSPPS(pkt)

		if b.bridge != nil {
			receiver.ProcessVideoPacket(uint32(b.track.SSRC()), trackID, pkt, b.bridge)
		}

		pkt.PayloadType = rtp_utils.PayloadTypeH264
	} else if isAudio {
		pkt.PayloadType = rtp_utils.PayloadTypeOpus
	}

	stats.packetCount++
	stats.logIfTime(isVideo)
}

func (b *TrackBroadcaster) broadcastToReceivers(pkt *rtp.Packet) {
	b.mu.RLock()
	workers := make([]*receiverWorker, 0, len(b.receiversVar))
	for _, w := range b.receiversVar {
		workers = append(workers, w)
	}
	b.mu.RUnlock()

	if len(workers) == 0 {
		return
	}

	for _, w := range workers {
		select {
		case w.ch <- pkt:
		default:
		}
	}
}

func (w *receiverWorker) start() {
	for pkt := range w.ch {
		pktCopy := *pkt
		if err := w.track.WriteRTP(&pktCopy); err != nil {
			logger.Errorf("stream", "Error writing to receiver %s: %v", w.id, err)
		}
	}
}

func (b *TrackBroadcaster) extractSPSPPS(pkt *rtp.Packet) {
	payload := pkt.Payload
	if len(payload) == 0 {
		return
	}

	nalType := payload[0] & rtp_utils.NALMask

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
				unitType := unit[0] & rtp_utils.NALMask
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
