package sender

import (
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func NewOfferCallback(
	manager *stream.StreamManager,
	sigClient signaling.Service,
	webrtcConfig *webrtc.Configuration,
	conn *net.UDPConn,
	bridge *receiver.RTPBridge,
	isReceivingRemoteVideo *atomic.Bool,
	cfg *config.Config,
	clientID string,
) func(string, string) {
	return func(offer string, senderID string) {
		if len(offer) < 10 {
			return
		}
		logger.Debugf("sender", "Offer received: %s", senderID)
		go func() {
			if existing := manager.GetPeerConnection(senderID); existing != nil {
				state := existing.ConnectionState()
				if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
					logger.Debugf("sender", "Replacing failed/closed PC for Offer: %s", senderID)
					existing.Close()
					manager.RemovePeerConnection(senderID)
				} else {
					logger.Debugf("sender", "Reusing existing PC for Offer: %s", senderID)
				}
			}
			if err := HandleOfferAsReceiver(offer, senderID, sigClient, webrtcConfig, manager, conn, bridge, isReceivingRemoteVideo, cfg, clientID); err != nil {
				logger.Errorf("sender", "Offer handle error: %v", err)
			}
		}()
	}
}

func NewAnswerCallback(
	manager *stream.StreamManager,
	bridge *receiver.RTPBridge,
	pendingCandidates map[string][]webrtc.ICECandidateInit,
	mu *sync.Mutex,
) func(string, string) {
	return func(answer string, senderID string) {
		logger.Debugf("sender", "Answer received: %s", senderID)

		pc := manager.GetPeerConnection(senderID)
		if pc == nil {
			logger.Warnf("sender", "PC not found for Answer: %s", senderID)
			return
		}

		if pc.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
			logger.Warnf("sender", "Unexpected signaling state for Answer: %s (%s)", senderID, pc.SignalingState().String())
			return
		}

		var answerSDP webrtc.SessionDescription
		if err := json.Unmarshal([]byte(answer), &answerSDP); err != nil {
			logger.Errorf("sender", "Answer parse error: %v", err)
			return
		}

		receiver.ExtractSPSPPSFromSDP(answerSDP.SDP, bridge)

		if err := pc.SetRemoteDescription(answerSDP); err != nil {
			logger.Errorf("sender", "SetRemoteDescription error: %v", err)
			return
		}

		logger.Debugf("sender", "Answer set: %s", senderID)

		mu.Lock()
		candidates := pendingCandidates[senderID]
		delete(pendingCandidates, senderID)
		mu.Unlock()

		for _, cand := range candidates {
			if err := pc.AddICECandidate(cand); err != nil {
				logger.Warnf("sender", "ICE Candidate add error: %v", err)
			} else {
				logger.Debugf("sender", "Buffered ICE Candidate added: %s", senderID)
			}
		}

		go func() {
			WaitForStableAndForward(senderID, func() *webrtc.PeerConnection {
				return manager.GetPeerConnection(senderID)
			}, manager, time.Second, true)
		}()
	}
}

func NewCandidateCallback(
	manager *stream.StreamManager,
	pendingCandidates map[string][]webrtc.ICECandidateInit,
	mu *sync.Mutex,
) func(string, string) {
	return func(candidate string, senderID string) {
		var iceCandidate webrtc.ICECandidateInit
		if err := json.Unmarshal([]byte(candidate), &iceCandidate); err != nil {
			logger.Errorf("sender", "ICE Candidate parse error: %v", err)
			return
		}

		pc := manager.GetPeerConnection(senderID)
		if pc == nil {
			mu.Lock()
			pendingCandidates[senderID] = append(pendingCandidates[senderID], iceCandidate)
			mu.Unlock()
			return
		}

		if pc.RemoteDescription() == nil {
			mu.Lock()
			pendingCandidates[senderID] = append(pendingCandidates[senderID], iceCandidate)
			mu.Unlock()
			return
		}

		if err := pc.AddICECandidate(iceCandidate); err != nil {
			logger.Warnf("sender", "ICE Candidate add error: %v", err)
			return
		}

		logger.Debugf("sender", "ICE Candidate added: %s", senderID)
	}
}

func NewConnectionCallback(
	manager *stream.StreamManager,
	sigClient signaling.Service,
	webrtcConfig *webrtc.Configuration,
	conn *net.UDPConn,
	bridge *receiver.RTPBridge,
	isReceivingRemoteVideo *atomic.Bool,
	cfg *config.Config,
	clientID string,
) func(string) {
	return func(senderID string) {
		logger.Debugf("sender", "Connection request: %s (MyID: %s)", senderID, clientID)

		if clientID <= senderID {
			logger.Debugf("sender", "[Glare Avoidance] PeerID(%s) >= MyID(%s). Skip offer.", senderID, clientID)
			return
		}

		logger.Debugf("sender", "Creating offer as initiator")
		go func() {
			if err := CreatePeerConnection(senderID, sigClient, webrtcConfig, manager, conn, cfg, bridge, isReceivingRemoteVideo, clientID); err != nil {
				logger.Errorf("sender", "PC creation error: %v", err)
			} else {
				logger.Debugf("sender", "PC created: %s", senderID)
			}
		}()
	}
}

func NewDisconnectCallback(manager *stream.StreamManager) func(string) {
	return func(senderID string) {
		logger.Debugf("sender", "Disconnected: %s", senderID)
		if pc := manager.GetPeerConnection(senderID); pc != nil {
			pc.Close()
			manager.RemovePeerConnection(senderID)
		}
	}
}
