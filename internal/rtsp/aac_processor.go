package rtsp

import (
	"sync"

	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)

const (
	AACSizeLength       = 13
	AACIndexLength      = 3
	AACIndexDeltaLength = 3
)

type AACProcessor struct {
	converter *rtp_utils.OpusToAACConverter
	nextTS    uint32
	tsInit    bool
	mu        sync.Mutex
}

func NewAACProcessor() (*AACProcessor, error) {
	c, err := rtp_utils.NewOpusToAACConverter()
	if err != nil {
		return nil, err
	}
	return &AACProcessor{
		converter: c,
	}, nil
}

func (p *AACProcessor) Process(pkt *rtp.Packet) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.tsInit {
		p.nextTS = pkt.Timestamp
		p.tsInit = true
	}

	aacPayload, err := p.converter.Convert(pkt)
	if err != nil {
		return false, err
	}
	if len(aacPayload) == 0 {
		return false, nil
	}

	aacLen := len(aacPayload)
	rtpPayload := make([]byte, 4+aacLen)
	rtpPayload[0] = 0x00
	rtpPayload[1] = 0x10
	rtpPayload[2] = byte(aacLen >> 5)
	rtpPayload[3] = byte((aacLen & 0x1F) << 3)
	copy(rtpPayload[4:], aacPayload)

	pkt.Payload = rtpPayload
	pkt.PayloadType = rtp_utils.PayloadTypeAAC
	pkt.Timestamp = p.nextTS
	p.nextTS += 1024

	return true, nil
}

func (p *AACProcessor) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.converter != nil {
		p.converter.Close()
	}
}
