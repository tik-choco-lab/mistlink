package receiver

import (
	"time"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)

func (b *RTPBridge) WriteRTP(pkt *rtp.Packet) {
	b.mu.Lock()
	started := b.started
	b.mu.Unlock()

	if !started {
		return
	}

	payload := packetPool.Get().([]byte)
	if cap(payload) < len(pkt.Payload) {
		payload = make([]byte, len(pkt.Payload))
	}
	payload = payload[:len(pkt.Payload)]
	copy(payload, pkt.Payload)

	packetCopy := &rtp.Packet{
		Header:  pkt.Header,
		Payload: payload,
	}

	select {
	case b.rtpChan <- packetCopy:
	default:
		packetPool.Put(payload)
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
			var buf []*bufferedPacket
			var order *[]uint16

			if pkt.PayloadType == rtp_utils.PayloadTypeOpus {
				buf = b.audioBuffer
				order = &b.audioOrder
			} else {
				buf = b.videoBuffer
				order = &b.videoOrder
			}

			if len(*order) >= maxBufferSize {
				oldestSeq := (*order)[0]
				*order = (*order)[1:]
				if bpkt := buf[oldestSeq]; bpkt != nil {
					packetPool.Put(bpkt.pkt.Payload)
					buf[oldestSeq] = nil
				}
			}

			if old := buf[pkt.SequenceNumber]; old != nil {
				packetPool.Put(old.pkt.Payload)
			} else {
				*order = append(*order, pkt.SequenceNumber)
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
			b.bufferMu.Lock()
			b.cleanupBuffer(b.videoBuffer, &b.videoOrder)
			b.cleanupBuffer(b.audioBuffer, &b.audioOrder)
			b.bufferMu.Unlock()
			return
		}
	}
}

func (b *RTPBridge) cleanupBuffer(buf []*bufferedPacket, order *[]uint16) {
	for _, seq := range *order {
		if bpkt := buf[seq]; bpkt != nil {
			packetPool.Put(bpkt.pkt.Payload)
			buf[seq] = nil
		}
	}
	*order = (*order)[:0]
}

func (b *RTPBridge) flushBufferedPackets() {
	b.mu.Lock()
	server := b.server
	b.mu.Unlock()

	if server == nil {
		return
	}

	b.flushBufferSet(rtp_utils.PayloadTypeH264, server)
	b.flushBufferSet(rtp_utils.PayloadTypeOpus, server)
}

func (b *RTPBridge) flushBufferSet(payloadType uint8, server *rtspserver.Server) {
	var toSend []*rtp.Packet

	b.bufferMu.Lock()

	var buf []*bufferedPacket
	var order *[]uint16
	if payloadType == rtp_utils.PayloadTypeOpus {
		buf = b.audioBuffer
		order = &b.audioOrder
	} else {
		buf = b.videoBuffer
		order = &b.videoOrder
	}

	if len(*order) == 0 {
		b.bufferMu.Unlock()
		return
	}

	nextSeq, exists := b.nextSeq[payloadType]
	if !exists {
		nextSeq = (*order)[0]
		b.nextSeq[payloadType] = nextSeq
	}

	sentInBatch := 0
	for sentInBatch < 500 && len(*order) > 0 {
		bpkt := buf[nextSeq]
		if bpkt == nil {
			oldestSeq := (*order)[0]
			oldestPkt := buf[oldestSeq]
			if oldestPkt != nil && time.Since(oldestPkt.received) > 100*time.Millisecond {
				nextSeq = oldestSeq
				b.nextSeq[payloadType] = nextSeq
				continue
			}
			break
		}

		switch payloadType {
		case rtp_utils.PayloadTypeH264:
			bpkt.pkt.SSRC = 0x12345678
		case rtp_utils.PayloadTypeOpus:
			bpkt.pkt.SSRC = 0x87654321
		}

		toSend = append(toSend, bpkt.pkt)

		buf[nextSeq] = nil

		if len(*order) > 0 && (*order)[0] == nextSeq {
			*order = (*order)[1:]
		} else {
			for i, seq := range *order {
				if seq == nextSeq {
					*order = append((*order)[:i], (*order)[i+1:]...)
					break
				}
			}
		}

		nextSeq++
		b.nextSeq[payloadType] = nextSeq
		sentInBatch++
	}
	b.bufferMu.Unlock()

	if len(toSend) > 0 {
		for _, pkt := range toSend {
			if err := server.WritePacketRTP(pkt); err != nil {
				logger.Warnf("RTSP", "RTSP send error: %v", err)
			}
			packetPool.Put(pkt.Payload)
		}
		logger.Debugf("RTSP", "Flushed %d packets for PT %d", len(toSend), payloadType)
	}
}
