package stream

import "github.com/pion/webrtc/v4"

func (m *StreamManager) AddPeerConnection(id string, pc *webrtc.PeerConnection) {
	m.pcMu.Lock()
	defer m.pcMu.Unlock()
	m.peerConnections[id] = pc
}

func (m *StreamManager) RemovePeerConnection(id string) {
	m.pcMu.Lock()
	defer m.pcMu.Unlock()
	delete(m.peerConnections, id)
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
