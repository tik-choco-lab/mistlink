package receiver

import (
	"time"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
)
 
const (
	flushInterval = 50 * time.Millisecond
)

func (b *RTPBridge) WriteRTP(pkt *rtp.Packet) {
	b.mu.Lock()
	allowed := false
	if (b.primaryVideoSSRC != 0 && pkt.SSRC == b.primaryVideoSSRC) || (b.primaryAudioSSRC != 0 && pkt.SSRC == b.primaryAudioSSRC) {
		allowed = true
	}
	started := b.started
	b.mu.Unlock()

	if !allowed {
		return
	}

	b.Broadcast(pkt)

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

	const (
		minInterval = 10 * time.Millisecond
		maxInterval = 100 * time.Millisecond
		checkWindow = 1 * time.Second
	)

	currentInterval := flushInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	checkTicker := time.NewTicker(checkWindow)
	defer checkTicker.Stop()

	packetCount := 0

	for {
		select {
		case pkt := <-b.rtpChan:
			b.rtspBuffer.Add(pkt)
			b.flushBufferedPackets()
			packetCount++

		case <-checkTicker.C:
			pps := packetCount
			packetCount = 0

			var newInterval time.Duration
			if pps > 0 {
				calculated := time.Duration(1000/pps/2) * time.Millisecond
				if calculated < minInterval {
					newInterval = minInterval
				} else if calculated > maxInterval {
					newInterval = maxInterval
				} else {
					newInterval = calculated
				}
			} else {
				newInterval = maxInterval
			}

			if newInterval != currentInterval {
				currentInterval = newInterval
				ticker.Reset(currentInterval)
				logger.Debugf("RTSP", "Adaptive buffering: PPS=%d, NewInterval=%v", pps, currentInterval)
			}

		case <-ticker.C:
			b.flushBufferedPackets()

		case <-b.stopChan:
			b.flushBufferedPackets()
			b.rtspBuffer.Cleanup()
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

	b.rtspBuffer.Flush(server)
}

func (b *RTPBridge) AddListener(cb func(*rtp.Packet)) int {
	b.listenerMu.Lock()
	defer b.listenerMu.Unlock()
	id := b.nextListenerID
	b.nextListenerID++
	b.packetListeners[id] = cb
	return id
}

func (b *RTPBridge) RemoveListener(id int) {
	b.listenerMu.Lock()
	defer b.listenerMu.Unlock()
	delete(b.packetListeners, id)
}

func (b *RTPBridge) Broadcast(pkt *rtp.Packet) {
	b.listenerMu.RLock()
	listeners := make([]func(*rtp.Packet), 0, len(b.packetListeners))
	for _, l := range b.packetListeners {
		listeners = append(listeners, l)
	}
	b.listenerMu.RUnlock()

	for _, l := range listeners {
		l(pkt)
	}
}
