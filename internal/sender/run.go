package sender

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/receiver"
	"github.com/tik-choco-lab/mistlink/internal/signaling"
	"github.com/tik-choco-lab/mistlink/internal/stream"
)

func Run(cfg *config.Config) error {
	addr, err := parseUDPAddr(cfg.InputURL)
	if err != nil {
		return fmt.Errorf("udp addr parse error: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("udp listen error: %w", err)
	}
	defer conn.Close()

	logger.Debugf("sender", "UDP listening on: %s", addr.String())

	clientID := uuid.New().String()
	logger.Debugf("sender", "Connecting to signaling server: %s (Room: %s, ClientID: %s)", cfg.SignalingServer, cfg.RoomID, clientID)
	sigClient, err := signaling.NewClient(cfg, clientID)
	if err != nil {
		return fmt.Errorf("signaling client error: %w", err)
	}
	defer sigClient.Close()
	logger.Debugf("sender", "Connected to signaling server")

	webrtcConfig := webrtc.Configuration{
		ICEServers: cfg.ICEServers,
	}
	re := regexp.MustCompile(`:(\d+)`)
	matches := re.FindStringSubmatch(cfg.RTSPURL)
	if len(matches) != 2 {
		return fmt.Errorf("rtsp port parse error: %w", err)
	}
	rtspPort := matches[1]
	rtspPortInt, err := strconv.Atoi(rtspPort)
	if err != nil {
		return fmt.Errorf("rtsp port parse error: %w", err)
	}
	bridge, err := receiver.NewRTPBridge(rtspPortInt, 5000)
	if err != nil {
		return fmt.Errorf("rtsp server start error: %w", err)
	}
	defer bridge.Stop()

	manager := stream.NewStreamManager(cfg, sigClient, bridge)
	pendingCandidates := make(map[string][]webrtc.ICECandidateInit)
	var pendingCandidatesMu sync.Mutex

	var isReceivingRemoteVideo atomic.Bool

	matches = re.FindStringSubmatch(cfg.WHIPURL)
	if len(matches) != 2 {
		return fmt.Errorf("whip port parse error: %w", err)
	}
	whipPort := matches[1]
	go func() {
		if err := StartWHIPServer(fmt.Sprintf(":%s", whipPort), &webrtcConfig, manager, bridge, cfg); err != nil {
			logger.Errorf("sender", "WHIP server error: %v", err)
		}
	}()

	configurer := &PeerConnectionConfigurer{
		sigClient:              sigClient,
		manager:                manager,
		udpConn:                conn,
		cfg:                    cfg,
		bridge:                 bridge,
		isReceivingRemoteVideo: &isReceivingRemoteVideo,
		webrtcConfig:           &webrtcConfig,
		clientID:               clientID,
	}

	sigClient.SetCallbacks(
		NewOfferCallback(configurer),
		NewAnswerCallback(configurer, pendingCandidates, &pendingCandidatesMu),
		NewCandidateCallback(manager, pendingCandidates, &pendingCandidatesMu),
		NewConnectionCallback(configurer),
		NewDisconnectCallback(manager),
	)

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("RoomID: %s\n", cfg.RoomID)
	fmt.Printf("RTSP URL: %s\n", cfg.RTSPURL)
	fmt.Printf("WHIP URL: %s\n", cfg.WHIPURL)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("(Ctrl+C to exit)")

	select {}
}

func parseUDPAddr(url string) (*net.UDPAddr, error) {
	if !strings.HasPrefix(url, "udp://") {
		return nil, fmt.Errorf("UDP URL required: %s", url)
	}

	addrStr := strings.TrimPrefix(url, "udp://")
	return net.ResolveUDPAddr("udp", addrStr)
}
