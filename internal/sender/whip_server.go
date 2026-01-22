package sender

import (
	"encoding/json"
	"io"
	"net/http"
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
	addr string,
	webrtcConfig *webrtc.Configuration,
	manager *stream.StreamManager,
	bridge *receiver.RTPBridge,
	cfg *config.Config,
) error {
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

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Debugf("sender", "WHIP server started: http://localhost%s/whip", addr)
	return server.ListenAndServe()
}
