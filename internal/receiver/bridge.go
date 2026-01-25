package receiver

import (
	"strings"
	"sync"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)
 
const (
	defaultBufferSize = 2000
)

type RTPBridge struct {
	rtspHost   string
	rtspPort   int
	audioCodec string

	mu      sync.Mutex
	sps     []byte
	pps     []byte
	started bool
	server  *rtspserver.Server

	rtpChan  chan *rtp.Packet
	stopChan chan struct{}
	wg       sync.WaitGroup

	rtspBuffer *RTSPBuffer

	activeTracks map[uint32]string // SSRC -> Type
	pliMu        sync.Mutex
	pliHandlers  map[uint32]func()

	listenerMu      sync.RWMutex
	packetListeners map[int]func(*rtp.Packet)
	nextListenerID  int

	primaryVideoSSRC uint32
	primaryAudioSSRC uint32
}

func NewRTPBridge(rtspHost string, rtspPort int, bufferSize int, audioCodec string) (*RTPBridge, int, error) {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	b := &RTPBridge{
		rtspHost:        rtspHost,
		rtspPort:        rtspPort,
		audioCodec:      audioCodec,
		rtspBuffer:      NewRTSPBuffer(bufferSize),
		rtpChan:         make(chan *rtp.Packet, bufferSize),
		stopChan:        make(chan struct{}),
		activeTracks:    make(map[uint32]string),
		pliHandlers:     make(map[uint32]func()),
		packetListeners: make(map[int]func(*rtp.Packet)),
	}
	b.wg.Add(1)
	go b.rtpSenderLoop()
	server, actualPort, err := rtspserver.StartServer(rtspHost, rtspPort, audioCodec)
	if err != nil {
		close(b.stopChan)
		b.wg.Wait()
		return nil, 0, err
	}
	b.server = server
	b.rtspPort = actualPort
	b.server.OnPlayCallback = b.RequestIDR
	return b, actualPort, nil
}

func (b *RTPBridge) Stop() {
	close(b.stopChan)
	b.wg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.server != nil {
		b.server.Close()
		b.server = nil
	}
	b.started = false
}

func (b *RTPBridge) TrackStarted(ssrc uint32, mimeType string) {
	logger.Debugf("receiver", "[Bridge] Track Started: %s (SSRC: %d)", mimeType, ssrc)
	b.mu.Lock()
	b.activeTracks[ssrc] = mimeType

	isAudio := strings.EqualFold(mimeType, "audio/opus") || strings.EqualFold(mimeType, "audio/aac")
	isVideo := strings.HasPrefix(strings.ToLower(mimeType), "video/") && (strings.Contains(strings.ToLower(mimeType), "h264") || strings.Contains(strings.ToLower(mimeType), "avc"))

	if isVideo {
		if b.primaryVideoSSRC == 0 {
			b.primaryVideoSSRC = ssrc
			logger.Debugf("receiver", "Set primary video SSRC: %d", ssrc)
		} else if b.primaryVideoSSRC != ssrc {
			logger.Infof("receiver", "Switching primary video SSRC: %d -> %d", b.primaryVideoSSRC, ssrc)
			b.primaryVideoSSRC = ssrc
		}
	} else if isAudio {
		if b.primaryAudioSSRC == 0 {
			b.primaryAudioSSRC = ssrc
			logger.Debugf("receiver", "Set primary audio SSRC: %d", ssrc)
		} else if b.primaryAudioSSRC != ssrc {
			logger.Infof("receiver", "Switching primary audio SSRC: %d -> %d", b.primaryAudioSSRC, ssrc)
			b.primaryAudioSSRC = ssrc
		}
	}

	b.mu.Unlock()

	if isVideo {
		b.RequestIDR()
	}
}

func (b *RTPBridge) RegisterPLIHandler(ssrc uint32, handler func()) {
	b.pliMu.Lock()
	defer b.pliMu.Unlock()
	b.pliHandlers[ssrc] = handler
}

func (b *RTPBridge) RequestIDR() {
	b.pliMu.Lock()
	handlers := make([]func(), 0, len(b.pliHandlers))
	for _, h := range b.pliHandlers {
		handlers = append(handlers, h)
	}
	b.pliMu.Unlock()

	for _, h := range handlers {
		go h()
	}
}

func (b *RTPBridge) IsStarted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started
}

func (b *RTPBridge) TrackStopped(ssrc uint32) {
	b.mu.Lock()
	delete(b.activeTracks, ssrc)

	if b.primaryVideoSSRC == ssrc {
		b.primaryVideoSSRC = 0
		logger.Debugf("receiver", "Primary video SSRC stopped: %d", ssrc)
	}
	if b.primaryAudioSSRC == ssrc {
		b.primaryAudioSSRC = 0
		logger.Debugf("receiver", "Primary audio SSRC stopped: %d", ssrc)
	}

	b.mu.Unlock()

	b.pliMu.Lock()
	delete(b.pliHandlers, ssrc)
	b.pliMu.Unlock()

	b.mu.Lock()
	if len(b.activeTracks) == 0 {
		if b.server != nil {
			b.server.StopRealData()
		}
		b.started = false
		b.sps = nil
		b.pps = nil
	}
	b.mu.Unlock()
}

func (b *RTPBridge) StopChan() <-chan struct{} {
	return b.stopChan
}
