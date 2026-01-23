package receiver

import (
	"sync"
	"time"

	"github.com/pion/rtp"
	rtspserver "github.com/tik-choco-lab/mistlink/internal/rtsp"
)

type bufferedPacket struct {
	pkt         *rtp.Packet
	received    time.Time
	payloadType uint8
}

var packetPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 2048) // MTU size
	},
}

type RTPBridge struct {
	rtspPort int

	mu      sync.Mutex
	sps     []byte
	pps     []byte
	started bool
	server  *rtspserver.Server

	rtpChan  chan *rtp.Packet
	stopChan chan struct{}
	wg       sync.WaitGroup

	videoBuffer []*bufferedPacket
	videoOrder  []uint16
	audioBuffer []*bufferedPacket
	audioOrder  []uint16
	bufferMu    sync.Mutex
	nextSeq     map[uint8]uint16
	outgoingSeq map[uint8]uint16

	lastInputTimestamp  map[uint8]uint32
	lastOutputTimestamp map[uint8]uint32

	bufferSize int

	activeTracks map[uint32]string // SSRC -> Type
	pliMu        sync.Mutex
	pliHandlers  map[uint32]func()
}

func NewRTPBridge(rtspPort int, bufferSize int) (*RTPBridge, error) {
	if bufferSize <= 0 {
		bufferSize = 2000
	}
	b := &RTPBridge{
		rtspPort:            rtspPort,
		bufferSize:          bufferSize,
		rtpChan:             make(chan *rtp.Packet, bufferSize),
		stopChan:            make(chan struct{}),
		videoBuffer:         make([]*bufferedPacket, 65536),
		videoOrder:          make([]uint16, 0, bufferSize),
		audioBuffer:         make([]*bufferedPacket, 65536),
		audioOrder:          make([]uint16, 0, bufferSize),
		nextSeq:             make(map[uint8]uint16),
		outgoingSeq:         make(map[uint8]uint16),
		lastInputTimestamp:  make(map[uint8]uint32),
		lastOutputTimestamp: make(map[uint8]uint32),
		activeTracks:        make(map[uint32]string),
		pliHandlers:         make(map[uint32]func()),
	}
	b.wg.Add(1)
	go b.rtpSenderLoop()
	server, err := rtspserver.StartServer(rtspPort)
	if err != nil {
		close(b.stopChan)
		b.wg.Wait()
		return nil, err
	}
	b.server = server
	b.server.OnPlayCallback = b.RequestIDR
	return b, nil
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
	b.mu.Lock()
	b.activeTracks[ssrc] = mimeType
	b.mu.Unlock()

	// Initial PLI on start for video tracks
	if mimeType == "video/H264" || mimeType == "video/h264" || mimeType == "VIDEO/H264" {
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
