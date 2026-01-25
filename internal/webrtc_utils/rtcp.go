package webrtc_utils

import (
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

const rtcpBufferSize = 1500

func StartRTCPReadLoop(rtpSender *webrtc.RTPSender, onPLI func()) {
	go func() {
		rtcpBuf := make([]byte, rtcpBufferSize)
		for {
			n, _, rtcpErr := rtpSender.Read(rtcpBuf)
			if rtcpErr != nil {
				return
			}

			if onPLI != nil {
				pkts, err := rtcp.Unmarshal(rtcpBuf[:n])
				if err != nil {
					continue
				}
				for _, pkt := range pkts {
					if _, ok := pkt.(*rtcp.PictureLossIndication); ok {
						onPLI()
					}
				}
			}
		}
	}()
}
