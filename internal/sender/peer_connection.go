package sender

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
	"github.com/tik-choco-lab/mistlink/internal/stream"
	"github.com/tik-choco-lab/mistlink/internal/webrtc_utils"
)

func CreatePeerConnection(
	receiverID string,
	sigClient signaling.Service,
	config *webrtc.Configuration,
	manager *stream.StreamManager,
	udpConn *net.UDPConn,
	cfg *config.Config,
	bridge *receiver.RTPBridge,
	isReceivingRemoteVideo *atomic.Bool,
	clientID string,
) error {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return err
	}

	i := &interceptor.Registry{}

	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return err
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i))
	pc, err := api.NewPeerConnection(*config)
	if err != nil {
		return err
	}

	manager.AddPeerConnection(receiverID, pc)

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		logger.Debugf("sender", "ICE State [%s]: %s", receiverID, state.String())
	})

	pc.OnSignalingStateChange(func(state webrtc.SignalingState) {
		logger.Debugf("sender", "Signaling State [%s]: %s", receiverID, state.String())
	})

	iceGatheringComplete := make(chan struct{})

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
	configurer.Configure(pc, receiverID, iceGatheringComplete, func() {
		go func() {
			WaitForStableAndForward(receiverID, func() *webrtc.PeerConnection {
				return pc
			}, manager, time.Second, false)
		}()
	})
	if err := configurer.EnsureOutgoingTracks(receiverID, pc, false, true); err != nil {
		pc.Close()
		manager.RemovePeerConnection(receiverID)
		return err
	}

	logger.Debugf("sender", "Creating Offer: %s", receiverID)
	if err := webrtc_utils.CreateAndSendOffer(pc, receiverID, sigClient); err != nil {
		pc.Close()
		return err
	}
	logger.Debugf("sender", "Offer sent: %s", receiverID)

	return nil
}
