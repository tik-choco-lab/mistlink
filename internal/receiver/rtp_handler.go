package receiver

import (
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)

func HandleTrack(track *webrtc.TrackRemote, rtcpWriter func([]rtcp.Packet) error, bridge *RTPBridge) {
	isVideo := track.Codec().MimeType == webrtc.MimeTypeH264
	isAudio := track.Codec().MimeType == webrtc.MimeTypeOpus

	if !isVideo && !isAudio {
		return
	}

	ssrc := uint32(track.SSRC())
	bridge.TrackStarted(ssrc, track.Codec().MimeType)
	defer bridge.TrackStopped(ssrc)

	if isVideo {
		bridge.RegisterPLIHandler(ssrc, func() {
			SendPLI(rtcpWriter, track.SSRC())
		})
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("receiver", "HandleTrack panic: %v", r)
		}
	}()

	logger.Debugf("receiver", "Track received: %s (SSRC: %d)", track.Codec().MimeType, track.SSRC())
	stats := newTrackStats()

	lastPLITime := time.Now()
	for {
		track.SetReadDeadline(time.Now().Add(5 * time.Second))
		pkt, _, err := track.ReadRTP()
		if err != nil {
			logger.Errorf("receiver", "Track read error: %v", err)
			return
		}

		if isVideo {
			missing := stats.checkSequenceGap(pkt.SequenceNumber)
			if len(missing) > 0 {
				SendNACK(rtcpWriter, track.SSRC(), missing)
			}
			if !bridge.IsStarted() && time.Since(lastPLITime) > 5*time.Second {
				SendPLI(rtcpWriter, track.SSRC())
				lastPLITime = time.Now()
			}
			nalType, isIDR := ProcessVideoPacket(pkt, bridge)
			stats.updateNALStats(nalType, isIDR)
			pkt.PayloadType = rtp_utils.PayloadTypeH264
		} else if isAudio {
			pkt.PayloadType = rtp_utils.PayloadTypeOpus
		}

		bridge.WriteRTP(pkt)
		stats.packetCount++
		stats.logIfTime(isVideo)
	}
}

func ProcessVideoPacket(pkt *rtp.Packet, bridge *RTPBridge) (byte, bool) {
	payload := pkt.Payload
	if len(payload) == 0 {
		return 0, false
	}

	nalType := payload[0] & 0x1F
	isIDR := false

	switch nalType {
	case rtp_utils.NALTypeIDR:
		isIDR = true
	case rtp_utils.NALTypeFUA:
		if len(payload) > 1 {
			orig := payload[1] & 0x1F
			start := (payload[1] & 0x80) != 0
			if orig == rtp_utils.NALTypeIDR && start {
				isIDR = true
			}
		}
	case rtp_utils.NALTypeSTAPA:
		pos := 1
		for pos+2 <= len(payload) {
			size := int(payload[pos])<<8 | int(payload[pos+1])
			pos += 2
			if pos+size > len(payload) {
				break
			}
			unit := payload[pos : pos+size]
			if len(unit) > 0 && (unit[0]&0x1F) == rtp_utils.NALTypeIDR {
				isIDR = true
				break
			}
			pos += size
		}
	}

	if isIDR {
		logger.Debugf("receiver", "IDR Frame: ts=%d, seq=%d", pkt.Timestamp, pkt.SequenceNumber)
	}

	bridge.ExtractSPSPPS(payload, nalType)
	return nalType, isIDR
}

func SendNACK(rtcpWriter func([]rtcp.Packet) error, ssrc webrtc.SSRC, missing []uint16) {
	if rtcpWriter == nil || len(missing) == 0 {
		return
	}

	nack := &rtcp.TransportLayerNack{
		MediaSSRC: uint32(ssrc),
	}

	for i := 0; i < len(missing); {
		base := missing[i]
		bitmap := uint16(0)
		j := i + 1
		for j < len(missing) {
			diff := missing[j] - base
			if diff > 16 {
				break
			}
			bitmap |= (1 << (diff - 1))
			j++
		}
		nack.Nacks = append(nack.Nacks, rtcp.NackPair{PacketID: base, LostPackets: rtcp.PacketBitmap(bitmap)})
		i = j
	}

	if err := rtcpWriter([]rtcp.Packet{nack}); err != nil {
		logger.Errorf("receiver", "Failed to send NACK: %v", err)
	}
}

func SendPLI(rtcpWriter func([]rtcp.Packet) error, ssrc webrtc.SSRC) {
	if rtcpWriter == nil {
		return
	}
	pli := &rtcp.PictureLossIndication{MediaSSRC: uint32(ssrc)}
	if err := rtcpWriter([]rtcp.Packet{pli}); err != nil {
		logger.Errorf("receiver", "Failed to send PLI: %v", err)
	} else {
		logger.Debugf("receiver", "Sent PLI to request keyframe (Bridge not started)")
	}
}
