package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pion/webrtc/v4"
	"github.com/tik-choco-lab/mistlink/internal/logger"
)

type Config struct {
	RoomID               string             `json:"room_id"`
	InputURL             string             `json:"input_url"`
	RTSPURL              string             `json:"rtsp_url"`
	WHIPURL              string             `json:"whip_url"`
	SignalingServer      string             `json:"signaling_server"`
	RTSPLoopback         bool               `json:"rtsp_loopback"`
	SPSPPSResendInterval int                `json:"spspps_resend_interval"`
	Audio                bool               `json:"audio"`
	AudioCodec           string             `json:"audio_codec"`
	ICEServers           []webrtc.ICEServer `json:"iceservers"`
	UseTUI               bool               `json:"use_tui"`
	ScreenCapture        bool               `json:"screen_capture"`
	CaptureTarget        string             `json:"capture_target"` // Window title or "entire"
	FrameRate            int                `json:"frame_rate"`
}

const (
	AudioCodecAAC  = "aac"
	AudioCodecOpus = "opus"
)

func DefaultConfig() *Config {
	return &Config{
		RoomID:               "",
		InputURL:             "udp://0.0.0.0:1234",
		RTSPURL:              "rtsp://localhost:8554/stream",
		WHIPURL:              "http://localhost:8080/whip",
		SignalingServer:      "wss://rtc.tik-choco.com/signaling",
		RTSPLoopback:         true,
		SPSPPSResendInterval: 1000,
		Audio:                true,
		AudioCodec:           AudioCodecAAC,
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
		FrameRate: 15,
	}
}

func Path() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		logger.Errorf("config", "Failed to get config directory: %v", err)
		return "", err
	}
	return filepath.Join(configDir, "mistlink", "config.json"), nil
}

func Load() (*Config, error) {
	configPath, err := Path()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		config := DefaultConfig()
		if err := Save(config); err != nil {
			return config, nil
		}
		return config, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Errorf("config", "Failed to read config file: %v", err)
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		logger.Errorf("config", "Failed to parse config file: %v", err)
		return nil, err
	}

	if len(config.ICEServers) == 0 {
		defaultConfig := DefaultConfig()
		config.ICEServers = defaultConfig.ICEServers
		if err := Save(&config); err != nil {
			logger.Errorf("config", "Failed to save updated config: %v", err)
		}
	}

	if config.AudioCodec != AudioCodecAAC && config.AudioCodec != AudioCodecOpus {
		logger.Warnf("config", "Invalid audio codec: %s. Defaulting to %s.", config.AudioCodec, AudioCodecAAC)
		config.AudioCodec = AudioCodecAAC
	}

	return &config, nil
}

func Save(config *Config) error {
	configPath, err := Path()
	if err != nil {
		return err
	}

	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		logger.Errorf("config", "Failed to create config directory: %v", err)
		return err
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		logger.Errorf("config", "Failed to marshal config: %v", err)
		return err
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		logger.Errorf("config", "Failed to write config file: %v", err)
		return err
	}

	return nil
}

func Show() error {
	path, err := Path()
	if err != nil {
		return err
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("Config: %s\n", path)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	return nil
}
