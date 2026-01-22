package stream

import (
	"sync"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/domain"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
)

type StreamManager struct {
	config    *config.Config
	sigClient domain.SignalingService
	bridge    *receiver.RTPBridge

	peerConnections map[string]*webrtc.PeerConnection
	pcMu            sync.RWMutex

	obsTracks map[*webrtc.TrackRemote]bool
	obsMu     sync.RWMutex

	forwardedReceivers map[string]map[*webrtc.TrackRemote]bool
	fwdMu              sync.RWMutex

	broadcasters map[*webrtc.TrackRemote]*TrackBroadcaster
	broadMu      sync.RWMutex
}

func NewStreamManager(cfg *config.Config, sig domain.SignalingService, bridge *receiver.RTPBridge) *StreamManager {
	return &StreamManager{
		config:             cfg,
		sigClient:          sig,
		bridge:             bridge,
		peerConnections:    make(map[string]*webrtc.PeerConnection),
		obsTracks:          make(map[*webrtc.TrackRemote]bool),
		forwardedReceivers: make(map[string]map[*webrtc.TrackRemote]bool),
		broadcasters:       make(map[*webrtc.TrackRemote]*TrackBroadcaster),
	}
}
