package sender

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func NewOfferCallback(c *PeerConnectionConfigurer) func(string, string) {
	return func(offer string, senderID string) {
		if len(offer) < 10 {
			return
		}
		logger.Debugf("sender", "Offer received: %s", senderID)
		go func() {
			if existing := c.manager.GetPeerConnection(senderID); existing != nil {
				state := existing.ConnectionState()
				if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
					logger.Debugf("sender", "Replacing failed/closed PC for Offer: %s", senderID)
					existing.Close()
					c.manager.RemovePeerConnection(senderID)
				} else {
					logger.Debugf("sender", "Reusing existing PC for Offer: %s", senderID)
				}
			}
			if err := HandleOfferAsReceiver(offer, senderID, c.sigClient, c.webrtcConfig, c.manager, c.udpConn, c.bridge, c.isReceivingRemoteVideo, c.cfg, c.clientID); err != nil {
				logger.Errorf("sender", "Offer handle error: %v", err)
			}
		}()
	}
}

func NewAnswerCallback(
	c *PeerConnectionConfigurer,
	pendingCandidates map[string][]webrtc.ICECandidateInit,
	mu *sync.Mutex,
) func(string, string) {
	return func(answer string, senderID string) {
		logger.Debugf("sender", "Answer received: %s", senderID)

		pc := c.manager.GetPeerConnection(senderID)
		if pc == nil {
			logger.Warnf("sender", "PC not found for Answer: %s", senderID)
			return
		}

		if pc.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
			logger.Warnf("sender", "Unexpected signaling state for Answer: %s (%s)", senderID, pc.SignalingState().String())
			return
		}

		var answerSDP webrtc.SessionDescription
		trimmed := strings.TrimSpace(answer)

		if strings.HasPrefix(trimmed, "{") {
			var helper struct {
				SDP string `json:"sdp"`
				S   string `json:"SDP"`
			}
			if err := json.Unmarshal([]byte(answer), &helper); err == nil {
				if helper.SDP != "" {
					answerSDP.SDP = helper.SDP
				} else {
					answerSDP.SDP = helper.S
				}
				answerSDP.Type = webrtc.SDPTypeAnswer
			}
		}

		if answerSDP.SDP == "" {
			if strings.Contains(trimmed, "v=0") || strings.Contains(trimmed, "o=-") {
				answerSDP.SDP = answer
				answerSDP.Type = webrtc.SDPTypeAnswer
			}
		}

		if answerSDP.SDP == "" {
			logger.Errorf("sender", "Invalid Answer format: %s", senderID)
			return
		}

		receiver.ExtractSPSPPSFromSDP(answerSDP.SDP, c.bridge)

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

		go WaitForStableAndForward(senderID, func() *webrtc.PeerConnection {
			return c.manager.GetPeerConnection(senderID)
		}, c, DefaultStableWaitTimeout, true)
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

func NewConnectionCallback(c *PeerConnectionConfigurer) func(string) {
	return func(senderID string) {
		logger.Debugf("sender", "Connection request: %s (MyID: %s)", senderID, c.clientID)

		if c.clientID <= senderID {
			logger.Debugf("sender", "[Glare Avoidance] PeerID(%s) >= MyID(%s). Skip offer.", senderID, c.clientID)
			return
		}

		if existing := c.manager.GetPeerConnection(senderID); existing != nil {
			state := existing.ConnectionState()
			if state == webrtc.PeerConnectionStateConnected || state == webrtc.PeerConnectionStateConnecting {
				logger.Debugf("sender", "PC already exists and is active, skipping connection request [%s]", senderID)
				return
			}
		}

		logger.Debugf("sender", "Creating offer as initiator")
		go func() {
			if err := CreatePeerConnection(senderID, c.sigClient, c.webrtcConfig, c.manager, c.udpConn, c.cfg, c.bridge, c.isReceivingRemoteVideo, c.clientID); err != nil {
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
