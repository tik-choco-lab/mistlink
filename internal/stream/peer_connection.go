package stream

import "github.com/pion/webrtc/v4"

func (m *StreamManager) AddPeerConnection(id string, pc *webrtc.PeerConnection) {
	m.pcMu.Lock()
	defer m.pcMu.Unlock()
	m.peerConnections[id] = pc
}

func (m *StreamManager) RemovePeerConnection(id string) {
	m.pcMu.Lock()
	delete(m.peerConnections, id)
	m.pcMu.Unlock()

	m.cleanupResources(id)
}

func (m *StreamManager) RemovePeerConnectionMatching(id string, matchPC *webrtc.PeerConnection) {
	m.pcMu.Lock()
	if m.peerConnections[id] != matchPC {
		m.pcMu.Unlock()
		return
	}
	delete(m.peerConnections, id)
	m.pcMu.Unlock()

	m.cleanupResources(id)
}

func (m *StreamManager) cleanupResources(id string) {
	m.fwdMu.Lock()
	delete(m.forwardedReceivers, id)
	m.fwdMu.Unlock()

	m.broadMu.RLock()
	for _, b := range m.broadcasters {
		b.RemoveReceiver(id)
	}
	m.broadMu.RUnlock()

	m.closeMu.Lock()
	handlers := m.closeHandlers[id]
	delete(m.closeHandlers, id)
	m.closeMu.Unlock()

	for _, h := range handlers {
		h()
	}
}

func (m *StreamManager) GetPeerConnection(id string) *webrtc.PeerConnection {
	m.pcMu.RLock()
	defer m.pcMu.RUnlock()
	return m.peerConnections[id]
}

func (m *StreamManager) GetAllReceiverIDs() []string {
	m.pcMu.RLock()
	defer m.pcMu.RUnlock()
	ids := make([]string, 0, len(m.peerConnections))
	for id := range m.peerConnections {
		ids = append(ids, id)
	}
	return ids
}
