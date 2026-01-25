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

	activeTracks map[uint32]int // SSRC -> TrackID
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
		activeTracks:    make(map[uint32]int),
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

func (b *RTPBridge) TrackStarted(ssrc uint32, mimeType string) int {
	logger.Debugf("receiver", "[Bridge] Track Started: %s (SSRC: %d)", mimeType, ssrc)
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextListenerID++
	trackID := b.nextListenerID
	
	b.activeTracks[ssrc] = trackID

	isAudio := strings.EqualFold(mimeType, "audio/opus") || strings.EqualFold(mimeType, "audio/aac")
	isVideo := strings.HasPrefix(strings.ToLower(mimeType), "video/") && (strings.Contains(strings.ToLower(mimeType), "h264") || strings.Contains(strings.ToLower(mimeType), "avc"))

	if isVideo {
		if b.primaryVideoSSRC != ssrc {
			logger.Debugf("receiver", "Switching primary video SSRC: %d -> %d (ID: %d)", b.primaryVideoSSRC, ssrc, trackID)
		} else {
			logger.Debugf("receiver", "Updating primary video track ID: %d (SSRC: %d)", trackID, ssrc)
		}
		b.primaryVideoSSRC = ssrc
	} else if isAudio {
		if b.primaryAudioSSRC != ssrc {
			logger.Debugf("receiver", "Switching primary audio SSRC: %d -> %d (ID: %d)", b.primaryAudioSSRC, ssrc, trackID)
		} else {
			logger.Debugf("receiver", "Updating primary audio track ID: %d (SSRC: %d)", trackID, ssrc)
		}
		b.primaryAudioSSRC = ssrc
	}

	if isVideo {
		go b.RequestIDR()
	}

	return trackID
}

func (b *RTPBridge) RegisterPLIHandler(ssrc uint32, handler func()) {
	b.pliMu.Lock()
	defer b.pliMu.Unlock()
	b.pliHandlers[ssrc] = handler
}

func (b *RTPBridge) UnregisterPLIHandler(ssrc uint32) {
	b.pliMu.Lock()
	defer b.pliMu.Unlock()
	delete(b.pliHandlers, ssrc)
}

func (b *RTPBridge) RequestIDR() {
	b.pliMu.Lock()
	handlers := make([]func(), 0, len(b.pliHandlers))
	for _, h := range b.pliHandlers {
		handlers = append(handlers, h)
	}
	b.pliMu.Unlock()

	for _, h := range handlers {
		h()
	}
}

func (b *RTPBridge) IsStarted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started
}

func (b *RTPBridge) TrackStopped(ssrc uint32, trackID int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentID, exists := b.activeTracks[ssrc]
	if !exists || currentID != trackID {
		logger.Warnf("receiver", "TrackStopped ignored for SSRC %d (ID: %d, Current: %d)", ssrc, trackID, currentID)
		return
	}

	delete(b.activeTracks, ssrc)

	if b.primaryVideoSSRC == ssrc {
		b.primaryVideoSSRC = 0
		logger.Debugf("receiver", "Primary video SSRC stopped: %d (ID: %d)", ssrc, trackID)
	}
	if b.primaryAudioSSRC == ssrc {
		b.primaryAudioSSRC = 0
		logger.Debugf("receiver", "Primary audio SSRC stopped: %d (ID: %d)", ssrc, trackID)
	}

	if len(b.activeTracks) == 0 {
		if b.server != nil {
			go b.server.StopRealData()
		}
		b.started = false
		b.sps = nil
		b.pps = nil
	}
}

func (b *RTPBridge) StopChan() <-chan struct{} {
	return b.stopChan
}
