package sender

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
)

const (
	videoCodec = 96  // Video (H.264)
	audioCodec = 111 // Audio (Opus)
)

func HandleMPEGTSStream(
	udpConn *net.UDPConn,
	videoTrack *webrtc.TrackLocalStaticRTP,
	audioTrack *webrtc.TrackLocalStaticRTP,
	bridge *receiver.RTPBridge,
	isReceivingRemoteVideo *atomic.Bool,
	rtspLoopback bool,
) {
	if udpConn == nil {
		logger.Errorf("sender", "udp connection is nil, cannot process RTP")
		return
	}

	logger.Debugf("sender", "UDP stream processing started: %s", udpConn.LocalAddr().String())

	packet := &rtp.Packet{}
	packetCount := 0
	lastLogTime := time.Now()
	buf := make([]byte, 1500)

	for {
		n, _, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			logger.Errorf("sender", "RTP read error: %v", err)
			return
		}

		if err := packet.Unmarshal(buf[:n]); err != nil {
			logger.Warnf("sender", "RTP parse error: %v (skip)", err)
			continue
		}

		if len(packet.Payload) == 0 {
			continue
		}

		switch packet.PayloadType {
		case videoCodec:
			if videoTrack != nil {
				if err := videoTrack.WriteRTP(packet); err != nil {
					logger.Errorf("sender", "video track write error: %v", err)
					return
				}
			}
		case audioCodec:
			if audioTrack != nil {
				if err := audioTrack.WriteRTP(packet); err != nil {
					logger.Errorf("sender", "audio track write error: %v", err)
					return
				}
			}
		}

		shouldBridge := bridge != nil && (rtspLoopback || isReceivingRemoteVideo == nil || !isReceivingRemoteVideo.Load())
		if shouldBridge {
			if packet.PayloadType == videoCodec {
				receiver.ProcessVideoPacket(packet, bridge)
			}
			bridge.WriteRTP(packet)
		}

		packetCount++
		if time.Since(lastLogTime) > 5*time.Second {
			packetCount = 0
			lastLogTime = time.Now()
		}
	}
}
