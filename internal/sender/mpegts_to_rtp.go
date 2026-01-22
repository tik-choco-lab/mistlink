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
	firstPacket := true

	for {
		buf := make([]byte, 1500)
		n, _, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			logger.Errorf("sender", "RTP read error: %v", err)
			return
		}
		rtpData := buf[:n]

		if firstPacket {
			logger.Debugf("sender", "First RTP packet received: size=%d bytes", len(rtpData))
			firstPacket = false
		}

		if err := packet.Unmarshal(rtpData); err != nil {
			logger.Warnf("sender", "RTP parse error: %v (skip)", err)
			continue
		}

		if len(packet.Payload) == 0 {
			continue
		}

		switch packet.PayloadType {
		case 96: // Video (H.264)
			if videoTrack != nil {
				if err := videoTrack.WriteRTP(packet); err != nil {
					logger.Errorf("sender", "video track write error: %v", err)
					return
				}
			}
		case 111: // Audio (Opus)
			if audioTrack != nil {
				if err := audioTrack.WriteRTP(packet); err != nil {
					logger.Errorf("sender", "audio track write error: %v", err)
					return
				}
			}
		}

		if bridge != nil && (rtspLoopback || isReceivingRemoteVideo == nil || !isReceivingRemoteVideo.Load()) {
			if packet.PayloadType == 96 {
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
