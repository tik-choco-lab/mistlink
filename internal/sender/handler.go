package sender

import (
	"encoding/json"
	"fmt"
	"net"
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
	if err := json.Unmarshal([]byte(offerStr), &offer); err != nil {
		return fmt.Errorf("Offer parse error: %w", err)
	}

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

	if err := pc.SetRemoteDescription(offer); err != nil {
		pc.Close()
		return err
	}

	logger.Debugf("sender", "Creating Answer: %s", senderID)
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return err
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return err
	}

	answerJSON, _ := json.Marshal(answer)
	if err := sigClient.SendAnswer(string(answerJSON), senderID); err != nil {
		pc.Close()
		return err
	}

	logger.Debugf("sender", "Answer sent: %s", senderID)
	return nil
}
