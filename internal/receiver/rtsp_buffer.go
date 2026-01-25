package receiver

import (
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)
 
const (
	defaultMTU           = 2048
	maxSequenceRange     = 65536
	minMaxBufferSize     = 1000
	maxFlushBatch        = 500
	maxStalePacketAge    = 100 * time.Millisecond
)

var packetPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultMTU)
	},
}

type bufferedPacket struct {
	pkt         *rtp.Packet
	received    time.Time
	payloadType uint8
}

type RTSPBuffer struct {
	bufferSize int
	mu         sync.Mutex

	videoBuffer []*bufferedPacket
	videoOrder  []uint16
	audioBuffer []*bufferedPacket
	audioOrder  []uint16

	nextSeq             map[uint8]uint16
	outgoingSeq         map[uint8]uint16
	lastInputTimestamp  map[uint8]uint32
	lastOutputTimestamp map[uint8]uint32
}

func NewRTSPBuffer(size int) *RTSPBuffer {
	if size <= 0 {
		size = 2000
	}
	return &RTSPBuffer{
		bufferSize:          size,
		videoBuffer:         make([]*bufferedPacket, maxSequenceRange),
		videoOrder:          make([]uint16, 0, size),
		audioBuffer:         make([]*bufferedPacket, maxSequenceRange),
		audioOrder:          make([]uint16, 0, size),
		nextSeq:             make(map[uint8]uint16),
		outgoingSeq:         make(map[uint8]uint16),
		lastInputTimestamp:  make(map[uint8]uint32),
		lastOutputTimestamp: make(map[uint8]uint32),
	}
}

func (b *RTSPBuffer) Add(pkt *rtp.Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var buf []*bufferedPacket
	var order *[]uint16

	if pkt.PayloadType == rtp_utils.PayloadTypeOpus {
		buf = b.audioBuffer
		order = &b.audioOrder
	} else {
		buf = b.videoBuffer
		order = &b.videoOrder
	}

	maxBufferSize := b.bufferSize / 2
	if maxBufferSize < minMaxBufferSize {
		maxBufferSize = minMaxBufferSize
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
}

func (b *RTSPBuffer) Flush(server *rtspserver.Server) {
	if server == nil {
		return
	}
	b.flushBufferSet(rtp_utils.PayloadTypeH264, server)
	b.flushBufferSet(rtp_utils.PayloadTypeOpus, server)
}

func (b *RTSPBuffer) Cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cleanupBuffer(b.videoBuffer, &b.videoOrder)
	b.cleanupBuffer(b.audioBuffer, &b.audioOrder)
}

func (b *RTSPBuffer) cleanupBuffer(buf []*bufferedPacket, order *[]uint16) {
	for _, seq := range *order {
		if bpkt := buf[seq]; bpkt != nil {
			packetPool.Put(bpkt.pkt.Payload)
			buf[seq] = nil
		}
	}
	*order = (*order)[:0]
}

func (b *RTSPBuffer) flushBufferSet(payloadType uint8, server *rtspserver.Server) {
	var toSend []*rtp.Packet

	b.mu.Lock()

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
		b.mu.Unlock()
		return
	}

	nextSeq, exists := b.nextSeq[payloadType]
	if !exists {
		nextSeq = (*order)[0]
		b.nextSeq[payloadType] = nextSeq
	}

	sentInBatch := 0
	for sentInBatch < maxFlushBatch && len(*order) > 0 {
		bpkt := buf[nextSeq]
		if bpkt == nil {
			oldestSeq := (*order)[0]
			oldestPkt := buf[oldestSeq]
			if oldestPkt != nil && time.Since(oldestPkt.received) > maxStalePacketAge {
				nextSeq = oldestSeq
				b.nextSeq[payloadType] = nextSeq
				continue
			}
			break
		}

		switch payloadType {
		case rtp_utils.PayloadTypeH264:
			bpkt.pkt.SSRC = rtp_utils.VideoSSRC
		case rtp_utils.PayloadTypeOpus:
			bpkt.pkt.SSRC = rtp_utils.AudioSSRC
		}

		// Rewrite Timestamps / Sequence Numbers
		outSeq := b.outgoingSeq[payloadType]
		lastInTS, _ := b.lastInputTimestamp[payloadType]
		lastOutTS, hasOutTS := b.lastOutputTimestamp[payloadType]

		originalTS := bpkt.pkt.Timestamp

		if !hasOutTS {
			b.lastOutputTimestamp[payloadType] = bpkt.pkt.Timestamp
			b.outgoingSeq[payloadType] = bpkt.pkt.SequenceNumber
			outSeq = bpkt.pkt.SequenceNumber
		} else {
			delta := originalTS - lastInTS
			if delta > rtp_utils.MaxTimestampDelta {
				delta = 0
			}
			newTS := lastOutTS + delta
			bpkt.pkt.Timestamp = newTS
			b.lastOutputTimestamp[payloadType] = newTS

			outSeq++
			bpkt.pkt.SequenceNumber = outSeq
			b.outgoingSeq[payloadType] = outSeq
		}
		b.lastInputTimestamp[payloadType] = originalTS

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
	b.mu.Unlock()

	if len(toSend) > 0 {
		for _, pkt := range toSend {
			if err := server.WritePacketRTP(pkt); err != nil {
				logger.Warnf("RTSP", "RTSP send error: %v", err)
			}
			packetPool.Put(pkt.Payload)
		}
		if payloadType == rtp_utils.PayloadTypeOpus {
			logger.Debugf("RTSP", "Flushed %d audio packets (PT %d)", len(toSend), payloadType)
		} else {
			logger.Debugf("RTSP", "Flushed %d video packets (PT %d)", len(toSend), payloadType)
		}
	}
}
