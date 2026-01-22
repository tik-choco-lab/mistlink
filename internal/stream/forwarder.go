package stream

import (
	"fmt"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/webrtc_utils"
)

func (m *StreamManager) ForwardTrackToReceiver(
	track *webrtc.TrackRemote,
	receiverPC *webrtc.PeerConnection,
	receiverID string,
) error {
	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		track.Codec().RTPCodecCapability,
		track.ID(),
		track.StreamID(),
	)
	if err != nil {
		return err
	}

	if receiverPC.LocalDescription() != nil {
		return fmt.Errorf("PeerConnection already has offer")
	}

	rtpSender, err := receiverPC.AddTrack(localTrack)
	if err != nil {
		return err
	}

	webrtc_utils.StartRTCPReadLoop(rtpSender)
	m.SubscribeToTrack(track, localTrack, receiverID)

	return nil
}

func (m *StreamManager) ForwardOBSTracksToReceiver(receiverID string, receiverPC *webrtc.PeerConnection, skipOffer bool) {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()

	if len(m.obsTracks) == 0 {
		return
	}

	m.fwdMu.Lock()
	forwardedTracks, exists := m.forwardedReceivers[receiverID]
	if !exists {
		forwardedTracks = make(map[*webrtc.TrackRemote]bool)
		m.forwardedReceivers[receiverID] = forwardedTracks
	}
	m.fwdMu.Unlock()

	var tracksToAdd []*webrtc.TrackRemote
	for track := range m.obsTracks {
		m.fwdMu.Lock()
		alreadyForwarded := forwardedTracks[track]
		m.fwdMu.Unlock()

		if alreadyForwarded {
			continue
		}
		tracksToAdd = append(tracksToAdd, track)
	}

	if len(tracksToAdd) == 0 {
		return
	}

	var localTracks []*webrtc.TrackLocalStaticRTP

	for _, track := range tracksToAdd {
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			track.Codec().RTPCodecCapability,
			track.ID(),
			track.StreamID(),
		)
		if err != nil {
			logger.Errorf("stream", "Track creation error %s: %v", receiverID, err)
			continue
		}

		rtpSender, err := receiverPC.AddTrack(localTrack)
		if err != nil {
			logger.Errorf("stream", "Track add error %s: %v", receiverID, err)
			continue
		}

		localTracks = append(localTracks, localTrack)
		webrtc_utils.StartRTCPReadLoop(rtpSender)
		m.SubscribeToTrack(track, localTrack, receiverID)

		m.fwdMu.Lock()
		forwardedTracks[track] = true
		m.fwdMu.Unlock()
	}

	if len(localTracks) > 0 {
		if skipOffer {
			return
		}

		signalingState := receiverPC.SignalingState()
		if signalingState != webrtc.SignalingStateStable {
			return
		}

		if err := webrtc_utils.CreateAndSendOffer(receiverPC, receiverID, m.sigClient); err != nil {
			logger.Errorf("stream", "Renegotiation offer error %s: %v", receiverID, err)
		}
	}
}

func (m *StreamManager) ForwardTrackToReceiverAfterConnection(
	track *webrtc.TrackRemote,
	receiverPC *webrtc.PeerConnection,
	receiverID string,
) error {
	signalingState := receiverPC.SignalingState()
	if signalingState != webrtc.SignalingStateStable {
		return fmt.Errorf("Signaling state not stable: %s", signalingState.String())
	}

	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		track.Codec().RTPCodecCapability,
		track.ID(),
		track.StreamID(),
	)
	if err != nil {
		return err
	}

	rtpSender, err := receiverPC.AddTrack(localTrack)
	if err != nil {
		return err
	}

	if receiverPC.SignalingState() != webrtc.SignalingStateStable {
		return nil
	}

	if err := webrtc_utils.CreateAndSendOffer(receiverPC, receiverID, m.sigClient); err != nil {
		return err
	}

	webrtc_utils.StartRTCPReadLoop(rtpSender)
	m.SubscribeToTrack(track, localTrack, receiverID)

	return nil
}

func (m *StreamManager) SubscribeToTrack(
	remoteTrack *webrtc.TrackRemote,
	localTrack *webrtc.TrackLocalStaticRTP,
	receiverID string,
) {
	m.broadMu.RLock()
	b := m.broadcasters[remoteTrack]
	m.broadMu.RUnlock()

	if b != nil {
		b.AddReceiver(receiverID, localTrack)
	} else {
		logger.Warnf("stream", "No broadcaster found for track %s when adding receiver %s", remoteTrack.ID(), receiverID)
	}
}
