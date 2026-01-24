package stream

import (
	"sync"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
)

type StreamManager struct {
	config    *config.Config
	sigClient signaling.Service
	bridge    *receiver.RTPBridge

	peerConnections map[string]*webrtc.PeerConnection
	pcMu            sync.RWMutex

	obsTracks map[*webrtc.TrackRemote]bool
	obsMu     sync.RWMutex

	forwardedReceivers map[string]map[*webrtc.TrackRemote]bool
	fwdMu              sync.RWMutex

	broadcasters map[*webrtc.TrackRemote]*TrackBroadcaster
	broadMu      sync.RWMutex

	closeHandlers map[string][]func()
	closeMu       sync.Mutex
}

func NewStreamManager(cfg *config.Config, sig signaling.Service, bridge *receiver.RTPBridge) *StreamManager {
	return &StreamManager{
		config:             cfg,
		sigClient:          sig,
		bridge:             bridge,
		peerConnections:    make(map[string]*webrtc.PeerConnection),
		obsTracks:          make(map[*webrtc.TrackRemote]bool),
		forwardedReceivers: make(map[string]map[*webrtc.TrackRemote]bool),
		broadcasters:       make(map[*webrtc.TrackRemote]*TrackBroadcaster),
		closeHandlers:      make(map[string][]func()),
	}
}

func (m *StreamManager) RegisterCloseHandler(id string, handler func()) {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.closeHandlers[id] = append(m.closeHandlers[id], handler)
}
