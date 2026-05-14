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
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func HandleOfferAsReceiver(
	offerStr string,
	senderID string,
	sigClient signaling.Service,
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

	sdpLen := len(offer.SDP)
	sdpSnippet := ""
	if sdpLen > 50 {
		sdpSnippet = offer.SDP[:50]
	} else {
		sdpSnippet = offer.SDP
	}
	logger.Debugf("sender", "Offer SDP (Len: %d): %s...", sdpLen, strings.ReplaceAll(sdpSnippet, "\n", " "))

	handleOffer := func(currentPC *webrtc.PeerConnection, isRetry bool) error {
		pc := currentPC
		isNewPC := false

		if pc == nil || pc.ConnectionState() == webrtc.PeerConnectionStateClosed || pc.ConnectionState() == webrtc.PeerConnectionStateFailed {
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

		if pc.SignalingState() != webrtc.SignalingStateStable && pc.SignalingState() != webrtc.SignalingStateClosed {
			if pc.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
				if err := pc.SetLocalDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeRollback}); err != nil {
					logger.Warnf("sender", "Rollback failed [%s]: %v", senderID, err)
				}
			}
		}

		if err := configurer.EnsureOutgoingTracks(senderID, pc, true, true); err != nil {
			pc.Close()
			manager.RemovePeerConnectionMatching(senderID, pc)
			return err
		}

		receiver.ExtractSPSPPSFromSDP(offer.SDP, bridge)

		if err := pc.SetRemoteDescription(offer); err != nil {
			if !isNewPC {
				return fmt.Errorf("RETRY_NEW_PC: %w", err)
			}
			pc.Close()
			manager.RemovePeerConnectionMatching(senderID, pc)
			return fmt.Errorf("SetRemoteDescription Error (new PC): %w", err)
		}

		logger.Debugf("sender", "Creating Answer: %s", senderID)
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			pc.Close()
			manager.RemovePeerConnectionMatching(senderID, pc)
			return fmt.Errorf("CreateAnswer Error: %w", err)
		}

		if err := pc.SetLocalDescription(answer); err != nil {
			pc.Close()
			manager.RemovePeerConnectionMatching(senderID, pc)
			return fmt.Errorf("SetLocalDescription Error: %w", err)
		}

		answerJSON, _ := json.Marshal(answer)
		if err := sigClient.SendAnswer(string(answerJSON), senderID); err != nil {
			logger.Errorf("sender", "SendAnswer Error [%s]: %v", senderID, err)
			return err
		}

		logger.Debugf("sender", "Answer sent: %s", senderID)
		return nil
	}

	existingPC := manager.GetPeerConnection(senderID)
	err := handleOffer(existingPC, false)
	if err != nil && strings.Contains(err.Error(), "RETRY_NEW_PC") {
		logger.Warnf("sender", "Failed to reuse PC for %s, closing and creating new one. Error: %v", senderID, err)
		if existingPC != nil {
			existingPC.Close()
			manager.RemovePeerConnectionMatching(senderID, existingPC)
		}
		if errRetry := handleOffer(nil, true); errRetry != nil {
			return fmt.Errorf("Retry failed: %w", errRetry)
		}
		return nil
	}

	return err
}
