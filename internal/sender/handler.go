package sender

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/domain"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func HandleOfferAsReceiver(
	offerStr string,
	senderID string,
	sigClient domain.SignalingService,
	config *webrtc.Configuration,
	manager *stream.StreamManager,
	udpConn *net.UDPConn,
	bridge *receiver.RTPBridge,
	isReceivingRemoteVideo *atomic.Bool,
	cfg *config.Config,
	clientID string,
) error {
	var offer webrtc.SessionDescription
	trimmed := strings.TrimSpace(offerStr)

	if strings.HasPrefix(trimmed, "{") {
		var helper struct {
			Type string `json:"type"`
			SDP  string `json:"sdp"`
			T    string `json:"Type"`
			S    string `json:"SDP"`
		}
		if err := json.Unmarshal([]byte(offerStr), &helper); err == nil {
			if helper.SDP != "" {
				offer.SDP = helper.SDP
			} else {
				offer.SDP = helper.S
			}

			typeName := helper.Type
			if typeName == "" {
				typeName = helper.T
			}
			if strings.EqualFold(typeName, "answer") {
				offer.Type = webrtc.SDPTypeAnswer
			} else {
				offer.Type = webrtc.SDPTypeOffer
			}
		}
	}

	if offer.SDP == "" {
		if strings.Contains(trimmed, "v=0") || strings.Contains(trimmed, "o=-") {
			offer.SDP = offerStr
			offer.Type = webrtc.SDPTypeOffer
		}
	}

	if offer.SDP == "" {
		return fmt.Errorf("failed to parse incoming offer: empty SDP or unknown format (length: %d)", len(offerStr))
	}

	logger.Debugf("sender", "Offer received (parsed). SDP Len: %d, Type: %s", len(offer.SDP), offer.Type.String())

	pc := manager.GetPeerConnection(senderID)
	isNewPC := false
	if pc == nil || pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		m := &webrtc.MediaEngine{}
		if err := m.RegisterDefaultCodecs(); err != nil {
			return err
		}

		i := &interceptor.Registry{}
		if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
			return err
		}

		api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i))
		var err error
		pc, err = api.NewPeerConnection(*config)
		if err != nil {
			return err
		}
		isNewPC = true
	} else {
		logger.Debugf("sender", "Reusing PC for Offer [%s]", senderID)
	}

	configurer := &PeerConnectionConfigurer{
		sigClient:              sigClient,
		manager:                manager,
		udpConn:                udpConn,
		cfg:                    cfg,
		bridge:                 bridge,
		isReceivingRemoteVideo: isReceivingRemoteVideo,
		clientID:               clientID,
		webrtcConfig:           config,
	}

	if isNewPC {
		configurer.Configure(pc, senderID, nil, nil)
		manager.AddPeerConnection(senderID, pc)
	}

	if pc.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
		if err := pc.SetLocalDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeRollback}); err != nil {
			logger.Errorf("sender", "Rollback failed [%s]: %v", senderID, err)
		}
	}

	if err := configurer.EnsureOutgoingTracks(senderID, pc, true, true); err != nil {
		pc.Close()
		return err
	}

	receiver.ExtractSPSPPSFromSDP(offer.SDP, bridge)

	vCount, aCount := 0, 0
	sdpLines := strings.Split(offer.SDP, "\n")
	for _, l := range sdpLines {
		line := strings.TrimSpace(l)
		if strings.HasPrefix(line, "m=video") {
			vCount++
		} else if strings.HasPrefix(line, "m=audio") {
			aCount++
		}
	}
	logger.Debugf("sender", "[Negotiation] Offer Statistics [%s]: video=%d, audio=%d", senderID, vCount, aCount)

	if err := pc.SetRemoteDescription(offer); err != nil {
		if isNewPC {
			pc.Close()
			return err
		}
		return fmt.Errorf("SetRemoteDescription Error: %w", err)
	}

	logger.Debugf("sender", "Creating Answer: %s", senderID)
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		if isNewPC {
			pc.Close()
			return err
		}
		return fmt.Errorf("CreateAnswer Error: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		if isNewPC {
			pc.Close()
			return err
		}
		return fmt.Errorf("SetLocalDescription Error: %w", err)
	}

	answerJSON, _ := json.Marshal(answer)
	if err := sigClient.SendAnswer(string(answerJSON), senderID); err != nil {
		pc.Close()
		return err
	}

	logger.Debugf("sender", "Answer sent: %s", senderID)
	return nil
}
