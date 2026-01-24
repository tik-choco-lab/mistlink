package sender

import (
	"encoding/json"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
	"github.com/tik-choco-lab/mistlink/internal/stream"
	"github.com/tik-choco-lab/mistlink/internal/webrtc_utils"
)

type PeerConnectionConfigurer struct {
	sigClient              signaling.Service
	manager                *stream.StreamManager
	udpConn                *net.UDPConn
	cfg                    *config.Config
	bridge                 *receiver.RTPBridge
	isReceivingRemoteVideo *atomic.Bool
	clientID               string
	webrtcConfig           *webrtc.Configuration
}

func (c *PeerConnectionConfigurer) Configure(
	pc *webrtc.PeerConnection,
	peerID string,
	iceGatheringComplete chan struct{},
	onConnected func(),
) {
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		mType := track.Codec().MimeType
		ssrc := uint32(track.SSRC())
		pt := track.PayloadType()
		logger.Debugf("sender", "[OnTrack] Remote Track Received: %s (PT: %d, SSRC: %d)", mType, pt, ssrc)

		c.isReceivingRemoteVideo.Store(true)

		if strings.EqualFold(mType, webrtc.MimeTypeH264) {
			go func() {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for i := 0; i < 3; i++ {
					requestKeyFrame(pc, track)
					select {
					case <-ticker.C:
					case <-c.bridge.StopChan():
						return
					}
				}
			}()
		}

		receiver.HandleTrack(track, pc.WriteRTCP, c.bridge)
	})

	pc.OnNegotiationNeeded(func() {
		state := pc.SignalingState()
		logger.Debugf("sender", "[OnNegotiationNeeded] Negotiation needed for %s (State: %s)", peerID, state.String())

		if state != webrtc.SignalingStateStable {
			return
		}
	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			candidateJSON := candidate.ToJSON()
			candidateJSONBytes, _ := json.Marshal(candidateJSON)
			if err := c.sigClient.SendCandidate(string(candidateJSONBytes), peerID); err == nil {
				logger.Debugf("sender", "ICE Candidate sent: %s", peerID)
			}
			return
		}

		if iceGatheringComplete != nil {
			logger.Debugf("sender", "ICE Gathering Complete: %s", peerID)
			closeIfOpen(iceGatheringComplete)
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger.Debugf("sender", "Connection State [%s]: %s", peerID, state.String())

		if state == webrtc.PeerConnectionStateConnected {
			logger.Debugf("sender", "Connected! [%s]", peerID)
			if onConnected != nil {
				onConnected()
			}
		}

		if state == webrtc.PeerConnectionStateFailed {
			c.cleanupPeerConnection(peerID, pc)
			go c.handleReconnect(peerID)
		}

		if state == webrtc.PeerConnectionStateDisconnected {
			c.scheduleReconnectIfStillDisconnected(peerID, pc)
		}

		if state == webrtc.PeerConnectionStateClosed {
			c.cleanupPeerConnection(peerID, pc)
		}
	})
}

func (c *PeerConnectionConfigurer) EnsureOutgoingTracks(
	peerID string,
	pc *webrtc.PeerConnection,
	skipIfHasSender bool,
	skipOffer bool,
) error {
	hasSender := peerConnectionHasSender(pc)
	connState := pc.ConnectionState()

	if skipIfHasSender && hasSender {
		if connState == webrtc.PeerConnectionStateConnected {
			logger.Debugf("sender", "PC already connected and has senders, skipping outgoing track addition [%s]", peerID)
			return nil
		}
		logger.Debugf("sender", "PC already has senders, checking for additional tracks [%s]", peerID)
	}

	if c.manager.HasOBSTracks() {
		logger.Debugf("sender", "Forwarding OBS tracks [%s]", peerID)
		c.manager.ForwardOBSTracksToReceiver(peerID, pc, skipOffer)
		return nil
	}

	if pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return nil
	}

	logger.Debugf("sender", "No OBS tracks. Using UDP track [%s]", peerID)
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
		"video",
		"mistlink",
	)
	if err != nil {
		pc.Close()
		return err
	}

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio",
		"mistlink",
	)
	if err != nil {
		pc.Close()
		return err
	}

	videoSender, err := pc.AddTrack(videoTrack)
	if err != nil {
		pc.Close()
		return err
	}
	webrtc_utils.StartRTCPReadLoop(videoSender)

	audioSender, err := pc.AddTrack(audioTrack)
	if err != nil {
		pc.Close()
		return err
	}
	webrtc_utils.StartRTCPReadLoop(audioSender)

	go HandleMPEGTSStream(c.udpConn, videoTrack, audioTrack, c.bridge, c.isReceivingRemoteVideo, c.cfg.RTSPLoopback)
	return nil
}

func (c *PeerConnectionConfigurer) handleReconnect(peerID string) {
	if c.sigClient == nil || c.manager == nil || c.webrtcConfig == nil {
		logger.Warnf("sender", "Reconnect skipped (missing dependencies) [%s]", peerID)
		return
	}

	if c.clientID != "" && c.clientID > peerID {
		logger.Debugf("sender", "Reconnecting as initiator: %s", peerID)
		if err := CreatePeerConnection(peerID, c.sigClient, c.webrtcConfig, c.manager, c.udpConn, c.cfg, c.bridge, c.isReceivingRemoteVideo, c.clientID); err != nil {
			logger.Errorf("sender", "Reconnect error: %v", err)
		}
		return
	}

	logger.Debugf("sender", "Reconnecting as receiver: %s", peerID)
	if err := c.sigClient.SendRequest(peerID); err != nil {
		logger.Errorf("sender", "Reconnect request error: %v", err)
	}
}

func (c *PeerConnectionConfigurer) scheduleReconnectIfStillDisconnected(peerID string, pc *webrtc.PeerConnection) {
	go func() {
		time.Sleep(3 * time.Second)
		currentPC := c.manager.GetPeerConnection(peerID)
		if currentPC == nil || currentPC != pc {
			return
		}

		if currentPC.ConnectionState() != webrtc.PeerConnectionStateDisconnected {
			return
		}

		logger.Debugf("sender", "[%s] Still disconnected after timeout, reconnecting...", peerID)
		c.cleanupPeerConnection(peerID, currentPC)
		c.handleReconnect(peerID)
	}()
}

func (c *PeerConnectionConfigurer) cleanupPeerConnection(peerID string, pc *webrtc.PeerConnection) {
	if pc != nil {
		pc.Close()
	}
	c.manager.RemovePeerConnection(peerID)
}

func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func peerConnectionHasSender(pc *webrtc.PeerConnection) bool {
	for _, sender := range pc.GetSenders() {
		if sender.Track() != nil {
			return true
		}
	}
	return false
}

func WaitForStableAndForward(
	receiverID string,
	getPC func() *webrtc.PeerConnection,
	manager *stream.StreamManager,
	timeout time.Duration,
	logOnStable bool,
) {
	startTime := time.Now()
	for {
		currentPC := getPC()
		if currentPC == nil {
			return
		}

		signalingState := currentPC.SignalingState()
		if signalingState == webrtc.SignalingStateStable {
			if logOnStable {
				logger.Debugf("sender", "Signaling Stable. Adding OBS tracks [%s]", receiverID)
			}
			manager.ForwardOBSTracksToReceiver(receiverID, currentPC, false)
			return
		}

		if timeout > 0 && time.Since(startTime) >= timeout {
			logger.Warnf("sender", "Wait for stable timeout [%s]", receiverID)
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func requestKeyFrame(pc *webrtc.PeerConnection, track *webrtc.TrackRemote) {
	err := pc.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{
			MediaSSRC: uint32(track.SSRC()),
		},
	})
	if err != nil {
		logger.Errorf("sender", "PLI send error: %v", err)
	} else {
		logger.Debugf("sender", "PLI sent")
	}
}
