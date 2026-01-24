package sender

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"

	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func StartWHIPServer(
	whipHost string,
	whipPort int,
	webrtcConfig *webrtc.Configuration,
	manager *stream.StreamManager,
	bridge *receiver.RTPBridge,
	cfg *config.Config,
) (int, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("/whip", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer}

		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/sdp") {
			offer.SDP = string(body)
		} else {
			if err := json.Unmarshal(body, &offer); err != nil {
				http.Error(w, "invalid SDP", http.StatusBadRequest)
				return
			}
		}

		m := &webrtc.MediaEngine{}
		if err := m.RegisterDefaultCodecs(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i := &interceptor.Registry{}
		if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i))
		pc, err := api.NewPeerConnection(*webrtcConfig)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		pc.OnTrack(func(track *webrtc.TrackRemote, rcv *webrtc.RTPReceiver) {
			logger.Debugf("sender", "WHIP: OBS Track received mime=%s ssrc=%d", track.Codec().MimeType, track.SSRC())
			manager.AddOBSTrack(track, pc.WriteRTCP)
		})

		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := pc.SetLocalDescription(answer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		gatherDone := webrtc.GatheringCompletePromise(pc)
		<-gatherDone

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/sdp")
		w.Header().Set("Location", "/whip/resource")
		w.WriteHeader(http.StatusCreated)

		if pc.LocalDescription() != nil {
			w.Write([]byte(pc.LocalDescription().SDP))
			return
		}
		respSDP, _ := json.Marshal(pc.LocalDescription())
		w.Write(respSDP)
	})

	currentPort := whipPort
	var ln net.Listener
	var err error

	for {
		bindHost := whipHost
		if bindHost == "localhost" {
			bindHost = ""
		}
		addr := net.JoinHostPort(bindHost, strconv.Itoa(currentPort))
		ln, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "address already in use") || strings.Contains(err.Error(), "bind: Only one usage") {
			logger.Warnf("sender", "WHIP Port %d already in use, trying next...", currentPort)
			currentPort++
			continue
		}
		return 0, err
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	displayHost := whipHost
	if displayHost == "" {
		displayHost = "localhost"
	}
	logger.Debugf("sender", "WHIP server started: http://%s:%d/whip", displayHost, currentPort)
	go server.Serve(ln)
	return currentPort, nil
}
