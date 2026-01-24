package webrtc_utils

import (
	"encoding/json"
	"fmt"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
)

func CreateAndSendOffer(receiverPC *webrtc.PeerConnection, receiverID string, sigClient signaling.Service) error {
	newOffer, err := receiverPC.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("negotiation offer creation error: %w", err)
	}

	if err := receiverPC.SetLocalDescription(newOffer); err != nil {
		return fmt.Errorf("negotiation offer setting error: %w", err)
	}

	if sigClient != nil {
		offerJSON, _ := json.Marshal(newOffer)
		if err := sigClient.SendOffer(string(offerJSON), receiverID); err != nil {
			return fmt.Errorf("negotiation offer sending error: %w", err)
		}
	}

	return nil
}
