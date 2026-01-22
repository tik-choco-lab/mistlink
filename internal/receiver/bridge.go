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

	videoBuffer map[uint16]*bufferedPacket
	audioBuffer map[uint16]*bufferedPacket
	bufferMu    sync.Mutex
	nextSeq     map[uint8]uint16
	outgoingSeq map[uint8]uint16

	lastInputTimestamp  map[uint8]uint32
	lastOutputTimestamp map[uint8]uint32

	bufferSize int

	activeTracks map[uint32]string // SSRC -> Type
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
		videoBuffer:         make(map[uint16]*bufferedPacket),
		audioBuffer:         make(map[uint16]*bufferedPacket),
		nextSeq:             make(map[uint8]uint16),
		outgoingSeq:         make(map[uint8]uint16),
		lastInputTimestamp:  make(map[uint8]uint32),
		lastOutputTimestamp: make(map[uint8]uint32),
		activeTracks:        make(map[uint32]string),
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
	defer b.mu.Unlock()
	b.activeTracks[ssrc] = mimeType
}

func (b *RTPBridge) IsStarted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started
}

func (b *RTPBridge) TrackStopped(ssrc uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.activeTracks, ssrc)

	if len(b.activeTracks) == 0 {
		if b.server != nil {
			b.server.StopRealData()
		}
		b.started = false
		b.sps = nil
		b.pps = nil
	}
}
