package sender

import (
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)

func HandleRelayStream(
	bridge *receiver.RTPBridge,
	videoTrack *webrtc.TrackLocalStaticRTP,
	audioTrack *webrtc.TrackLocalStaticRTP,
	done <-chan struct{},
) {
	if bridge == nil {
		logger.Errorf("sender", "Bridge is nil in HandleRelayStream")
		return
	}

	pktChan := make(chan *rtp.Packet, 100)

	listenerID := bridge.AddListener(func(pkt *rtp.Packet) {
		select {
		case pktChan <- pkt:
		default:
		}
	})

	logger.Debugf("sender", "Relay stream started. ListenerID: %d", listenerID)

	defer func() {
		bridge.RemoveListener(listenerID)
		logger.Debugf("sender", "Relay stream stopped. ListenerID: %d", listenerID)
	}()

	for {
		select {
		case <-done:
			return
		case pkt := <-pktChan:
			switch pkt.Header.PayloadType {
			case rtp_utils.PayloadTypeH264:
				if videoTrack != nil {
					if err := videoTrack.WriteRTP(pkt); err != nil {
					}
				}
			case rtp_utils.PayloadTypeOpus:
				if audioTrack != nil {
					if err := audioTrack.WriteRTP(pkt); err != nil {
					}
				}
			}
		}
	}
}
