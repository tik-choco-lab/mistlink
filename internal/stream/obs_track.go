package stream

import (
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
)

func (m *StreamManager) AddOBSTrack(track *webrtc.TrackRemote, rtcpWriter func([]rtcp.Packet) error) {
	m.broadMu.Lock()
	if _, exists := m.broadcasters[track]; !exists {
		var bridge *receiver.RTPBridge
		if m.config.RTSPLoopback {
			bridge = m.bridge
		}
		b := NewTrackBroadcaster(track, rtcpWriter, bridge)
		m.broadcasters[track] = b
		b.Start()
	}
	m.broadMu.Unlock()

	m.obsMu.Lock()
	m.obsTracks[track] = true
	m.obsMu.Unlock()

	receivers := m.GetAllReceiverIDs()

	for _, id := range receivers {
		pc := m.GetPeerConnection(id)
		if pc == nil {
			continue
		}

		if pc.LocalDescription() == nil {
			if err := m.ForwardTrackToReceiver(track, pc, id); err != nil {
				logger.Errorf("stream", "Error forwarding track to %s: %v", id, err)
			}
		} else {
			if pc.SignalingState() == webrtc.SignalingStateStable {
				if err := m.ForwardTrackToReceiverAfterConnection(track, pc, id); err != nil {
					logger.Errorf("stream", "Error forwarding track to %s (stable): %v", id, err)
				}
			}
		}
	}
}

func (m *StreamManager) HasOBSTracks() bool {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	return len(m.obsTracks) > 0
}
