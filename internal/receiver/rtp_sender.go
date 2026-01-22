package receiver

import (
	"time"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)

func (b *RTPBridge) WriteRTP(pkt *rtp.Packet) {
	b.mu.Lock()
	started := b.started
	b.mu.Unlock()

	if !started {
		return
	}

	packetCopy := &rtp.Packet{
		Header:  pkt.Header,
		Payload: make([]byte, len(pkt.Payload)),
	}
	copy(packetCopy.Payload, pkt.Payload)

	select {
	case b.rtpChan <- packetCopy:
	default:
		logger.Warnf("RTSP", "RTP channel full, dropping packet seq=%d", pkt.SequenceNumber)
	}
}

func (b *RTPBridge) rtpSenderLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	maxBufferSize := b.bufferSize / 2
	if maxBufferSize < 1000 {
		maxBufferSize = 1000
	}

	for {
		select {
		case pkt := <-b.rtpChan:
			b.bufferMu.Lock()
			buf := b.videoBuffer
			if pkt.PayloadType == 111 {
				buf = b.audioBuffer
			}

			if len(buf) >= maxBufferSize {
				var oldestSeq uint16
				oldestTime := time.Now()
				for seq, bpkt := range buf {
					if bpkt.received.Before(oldestTime) {
						oldestTime = bpkt.received
						oldestSeq = seq
					}
				}
				delete(buf, oldestSeq)
			}

			buf[pkt.SequenceNumber] = &bufferedPacket{
				pkt:         pkt,
				received:    time.Now(),
				payloadType: pkt.PayloadType,
			}
			b.bufferMu.Unlock()

		case <-ticker.C:
			b.flushBufferedPackets()

		case <-b.stopChan:
			b.flushBufferedPackets()
			return
		}
	}
}

func (b *RTPBridge) flushBufferedPackets() {
	b.mu.Lock()
	server := b.server
	b.mu.Unlock()

	if server == nil {
		return
	}

	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	videoCount := len(b.videoBuffer)
	audioCount := len(b.audioBuffer)

	b.flushBuffer(b.videoBuffer, 96, server)
	b.flushBuffer(b.audioBuffer, 111, server)

	if videoCount > 0 || audioCount > 0 {
		logger.Debugf("RTSP", "Flushed: video=%d, audio=%d", videoCount, audioCount)
	}
}

func (b *RTPBridge) flushBuffer(buf map[uint16]*bufferedPacket, payloadType uint8, server *rtspserver.Server) {
	if len(buf) == 0 {
		return
	}

	nextSeq, exists := b.nextSeq[payloadType]
	if !exists {
		nextSeq = 65535
		for seq := range buf {
			if seq < nextSeq {
				nextSeq = seq
			}
		}
		b.nextSeq[payloadType] = nextSeq
	}

	sent := 0
	for sent < 500 && len(buf) > 0 {
		bpkt, ok := buf[nextSeq]
		if !ok {
			var oldestSeq uint16
			oldestTime := time.Now()
			found := false
			for seq, pkt := range buf {
				if pkt.received.Before(oldestTime) {
					oldestTime = pkt.received
					oldestSeq = seq
					found = true
				}
			}

			if found && time.Since(oldestTime) > 100*time.Millisecond {
				delete(buf, oldestSeq)
				nextSeq++
				b.nextSeq[payloadType] = nextSeq
				continue
			}
			break
		}

		if payloadType == 96 {
			bpkt.pkt.SSRC = 0x12345678
		} else if payloadType == 111 {
			bpkt.pkt.SSRC = 0x87654321
		}

		if err := server.WritePacketRTP(bpkt.pkt); err != nil {
			logger.Warnf("RTSP", "RTSP send error: %v", err)
		}

		delete(buf, nextSeq)
		nextSeq++
		b.nextSeq[payloadType] = nextSeq
		sent++
	}
}
