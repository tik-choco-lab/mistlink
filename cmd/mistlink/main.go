package main

import (
	"flag"

	"github.com/tik-choco-lab/mistlink/internal/config"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/sender"
)

func main() {
	var (
		room          = flag.String("room", "", "Room ID")
		input         = flag.String("input", "", "Input source (e.g. udp://0.0.0.0:1234)")
		server        = flag.String("server", "", "MistNet signaling server URL")
		rtspServer    = flag.String("rtsp-server", "", "RTSP server URL")
		whipServer    = flag.String("whip-server", "", "WHIP server URL")
		debug         = flag.Bool("debug", false, "Enable debug logging")
		showConfig    = flag.Bool("show-config", false, "Show config")
		useTUI        = flag.Bool("tui", true, "Use TUI")
		screenCapture = flag.Bool("screen-capture", false, "Capture screen/window instead of UDP")
		captureTarget = flag.String("capture-target", "entire", "Window title or 'entire'")
		audioCapture  = flag.Bool("audio-capture", false, "Enable audio capture during screen sharing")
		audioSource   = flag.String("audio-source", "microphone", "Audio source: 'microphone' or 'system'")
	)
	flag.BoolVar(debug, "d", false, "Enable debug logging (alias)")
	flag.BoolVar(showConfig, "c", false, "Show config (alias)")
	flag.BoolVar(useTUI, "t", true, "Use TUI (alias)")
	flag.Parse()

	logger.InitWithOptions(logger.Options{
		Debug:          *debug,
		UseStderr:      !*useTUI,
		DisableConsole: *useTUI,
	})
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Errorf("main", "Failed to load config file: %v. Using defaults.", err)
		cfg = config.DefaultConfig()
		if err := config.Save(cfg); err != nil {
			logger.Errorf("main", "Failed to save config file: %v", err)
		}
	}

	if *showConfig {
		if err := config.Show(); err != nil {
			logger.Errorf("main", "Failed to show config: %v", err)
		}
		return
	}

	if *room != "" {
		cfg.RoomID = *room
	}
	if *input != "" {
		cfg.InputURL = *input
	}
	if *server != "" {
		cfg.SignalingServer = *server
	}

	if *rtspServer != "" {
		cfg.RTSPURL = *rtspServer
	}

	if *whipServer != "" {
		cfg.WHIPURL = *whipServer
	}

	cfg.UseTUI = *useTUI
	if *screenCapture {
		cfg.ScreenCapture = true
	}
	if *captureTarget != "entire" {
		cfg.CaptureTarget = *captureTarget
	}
	if *audioCapture {
		cfg.AudioCapture = true
	}
	if *audioSource != "microphone" {
		cfg.AudioSource = *audioSource
	}

	if cfg.SignalingServer == "" {
		cfg.SignalingServer = "wss://rtc.tik-choco.com/signaling"
	}

	if cfg.RoomID == "" {
		cfg.RoomID = generateRoomID()
	}

	if cfg.InputURL == "" {
		cfg.InputURL = "udp://0.0.0.0:1234"
	}

	if cfg.RTSPURL == "" {
		cfg.RTSPURL = "rtsp://localhost:8554/stream"
	}

	if cfg.WHIPURL == "" {
		cfg.WHIPURL = "http://localhost:8080/whip"
	}

	if err := config.Save(cfg); err != nil {
		logger.Errorf("main", "Failed to save config file: %v", err)
	}

	if err := sender.Run(cfg); err != nil {
		logger.Errorf("main", "Execution error: %v", err)
	}
}
