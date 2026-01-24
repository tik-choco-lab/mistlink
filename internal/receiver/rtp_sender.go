package receiver

import (
	"time"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
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

	for {
		select {
		case pkt := <-b.rtpChan:
			b.rtspBuffer.Add(pkt)

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
