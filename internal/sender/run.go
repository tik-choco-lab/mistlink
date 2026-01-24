package sender

import (
	"fmt"
	"net"
	"net/url"
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

const (
	DefaultRTSPPort  = 554
	DefaultRTSPSPort = 322
	DefaultHTTPPort  = 80
	DefaultHTTPSPort = 443
)

func Run(cfg *config.Config) error {
	addr, err := parseUDPAddr(cfg.InputURL)
	if err != nil {
		return fmt.Errorf("udp addr parse error: %w", err)
	}

	var conn *net.UDPConn
	for {
		conn, err = net.ListenUDP("udp", addr)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "address already in use") || strings.Contains(err.Error(), "bind: Only one usage") {
			logger.Warnf("sender", "UDP Port %d already in use, trying next...", addr.Port)
			addr.Port++
			continue
		}
		return fmt.Errorf("udp listen error: %w", err)
	}
	defer conn.Close()

	cfg.InputURL = fmt.Sprintf("udp://%s", addr.String())

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
	u, err := url.Parse(cfg.RTSPURL)
	if err != nil {
		return fmt.Errorf("rtsp url parse error: %w", err)
	}
	rtspHost, rtspPort, _ := net.SplitHostPort(u.Host)
	if rtspPort == "" {
		if u.Scheme == "rtsps" {
			rtspPort = strconv.Itoa(DefaultRTSPSPort)
		} else {
			rtspPort = strconv.Itoa(DefaultRTSPPort)
		}
	}
	rtspPortInt, _ := strconv.Atoi(rtspPort)

	bridge, actualRtspPort, err := receiver.NewRTPBridge(rtspHost, rtspPortInt, 5000, cfg.AudioCodec)
	if err != nil {
		return fmt.Errorf("rtsp server start error: %w", err)
	}
	defer bridge.Stop()

	if actualRtspPort != rtspPortInt {
		u.Host = net.JoinHostPort(rtspHost, strconv.Itoa(actualRtspPort))
		cfg.RTSPURL = u.String()
	}

	var isReceivingRemoteVideo atomic.Bool

	go HandleMPEGTSStream(conn, nil, nil, bridge, &isReceivingRemoteVideo, cfg.RTSPLoopback)

	manager := stream.NewStreamManager(cfg, sigClient, bridge)
	pendingCandidates := make(map[string][]webrtc.ICECandidateInit)
	var pendingCandidatesMu sync.Mutex

	uw, err := url.Parse(cfg.WHIPURL)
	if err != nil {
		return fmt.Errorf("whip url parse error: %w", err)
	}
	whipHost, whipPort, _ := net.SplitHostPort(uw.Host)
	if whipPort == "" {
		if uw.Scheme == "https" {
			whipPort = strconv.Itoa(DefaultHTTPSPort)
		} else {
			whipPort = strconv.Itoa(DefaultHTTPPort)
		}
	}
	whipPortInt, _ := strconv.Atoi(whipPort)

	actualWhipPort, err := StartWHIPServer(whipHost, whipPortInt, &webrtcConfig, manager, bridge, cfg)
	if err != nil {
		logger.Errorf("sender", "WHIP server error: %v", err)
	} else if actualWhipPort != 0 {
		if actualWhipPort != whipPortInt {
			uw.Host = net.JoinHostPort(whipHost, strconv.Itoa(actualWhipPort))
			cfg.WHIPURL = uw.String()
		}
	}

	sigClient.SetCallbacks(
		NewOfferCallback(manager, sigClient, &webrtcConfig, conn, bridge, &isReceivingRemoteVideo, cfg, clientID),
		NewAnswerCallback(manager, bridge, pendingCandidates, &pendingCandidatesMu),
		NewCandidateCallback(manager, pendingCandidates, &pendingCandidatesMu),
		NewConnectionCallback(manager, sigClient, &webrtcConfig, conn, bridge, &isReceivingRemoteVideo, cfg, clientID),
		NewRedirectCallback(sigClient),
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
