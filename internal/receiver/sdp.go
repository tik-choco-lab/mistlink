package receiver

import (
	"encoding/base64"
	"strings"

	"github.com/tik-choco-lab/mistlink/internal/logger"
)

func ExtractSPSPPSFromSDP(sdp string, bridge *RTPBridge) {
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "a=fmtp:") || !strings.Contains(line, "sprop-parameter-sets=") {
			continue
		}

		parts := strings.Split(line, "sprop-parameter-sets=")
		if len(parts) < 2 {
			continue
		}

		params := strings.Split(parts[1], ";")[0]
		sets := strings.Split(params, ",")
		if len(sets) < 2 {
			continue
		}

		sps, err1 := base64.StdEncoding.DecodeString(sets[0])
		pps, err2 := base64.StdEncoding.DecodeString(sets[1])
		if err1 != nil || err2 != nil || len(sps) == 0 || len(pps) == 0 {
			logger.Warnf("receiver", "⚠️ SDP SPS/PPS decode error: %v, %v", err1, err2)
			continue
		}

		logger.Infof("receiver", "📹 SDP SPS/PPS extracted: SPS=%d, PPS=%d", len(sps), len(pps))
		bridge.SetSPSPPS(sps, pps)
	}
}
