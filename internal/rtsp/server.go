package rtsp

import (
	"fmt"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"
	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)

type Server struct {
	srv            *gortsplib.Server
	stream         *gortsplib.ServerStream
	videoMedia     *description.Media
	audioMedia     *description.Media
	h264Format     *format.H264
	mutex          sync.RWMutex
	closeChan      chan struct{}
	hasRealData    bool
	OnPlayCallback func()
}

func StartServer(rtspPort int) (*Server, error) {
	s := &Server{
		closeChan: make(chan struct{}),
	}

	s.srv = &gortsplib.Server{
		RTSPAddress:    fmt.Sprintf(":%d", rtspPort),
		UDPRTPAddress:  ":8000",
		UDPRTCPAddress: ":8001",
		Handler:        s,
	}

	if err := s.srv.Start(); err != nil {
		return nil, fmt.Errorf("RTSP server start error: %w", err)
	}

	dummySPS := []byte{0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2}
	dummyPPS := []byte{0x68, 0xce, 0x3c, 0x80}

	s.initStreamInternal(dummySPS, dummyPPS, false)

	go s.dummyPacketLoop()

	logger.Debugf("rtsp", "RTSP server started: rtsp://localhost:%d/stream", rtspPort)

	go func() {
		if err := s.srv.Wait(); err != nil {
			logger.Errorf("rtsp", "RTSP server error: %v", err)
		}
	}()

	return s, nil
}

func (s *Server) initStreamInternal(sps []byte, pps []byte, isReal bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if isReal {
		s.hasRealData = true
	}

	if s.stream != nil {
		if s.h264Format != nil {
			s.h264Format.SPS = sps
			s.h264Format.PPS = pps
		}
		return
	}

	s.h264Format = &format.H264{
		PayloadTyp:        rtp_utils.PayloadTypeH264,
		PacketizationMode: 1,
		SPS:               sps,
		PPS:               pps,
	}

	opusFormat := &format.Opus{
		PayloadTyp: rtp_utils.PayloadTypeOpus,
	}

	desc := &description.Session{
		Medias: []*description.Media{
			{
				Type:    description.MediaTypeVideo,
				Formats: []format.Format{s.h264Format},
			},
			{
				Type:    description.MediaTypeAudio,
				Formats: []format.Format{opusFormat},
			},
		},
	}

	s.stream = gortsplib.NewServerStream(s.srv, desc)
	sdp, _ := desc.Marshal(false)
	logger.Debugf("rtsp", "RTSP Stream Initialized. SDP:\n%s", string(sdp))
	s.videoMedia = desc.Medias[0]
	s.audioMedia = desc.Medias[1]
}

func (s *Server) InitStream(sps []byte, pps []byte) error {
	logger.Debugf("rtsp", "InitStream: SPS/PPS received.")
	s.initStreamInternal(sps, pps, true)
	return nil
}

func (s *Server) StopRealData() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.hasRealData = false
	logger.Debugf("rtsp", "Real data stopped. Switching back to dummy loop.")
}

func (s *Server) dummyPacketLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var seq uint16
	var ts uint32
	ssrc := uint32(rtp_utils.VideoSSRC)

	for {
		select {
		case <-ticker.C:
			s.mutex.RLock()
			hasReal := s.hasRealData
			stream := s.stream
			videoMedia := s.videoMedia
			s.mutex.RUnlock()

			if hasReal || stream == nil || videoMedia == nil {
				continue
			}

			payload := []byte{0x0c, 0xff, 0xff, 0xff}
			pkt := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    rtp_utils.PayloadTypeH264,
					SequenceNumber: seq,
					Timestamp:      ts,
					SSRC:           ssrc,
					Marker:         true,
				},
				Payload: payload,
			}

			stream.WritePacketRTP(videoMedia, pkt)

			seq++
			ts += rtp_utils.DummyTimestampIncrement
		case <-s.closeChan:
			logger.Debugf("rtsp", "Dummy packet loop exited.")
			return
		}
	}
}

func (s *Server) WritePacketRTP(pkt *rtp.Packet) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.stream == nil {
		return nil
	}

	if pkt.PayloadType == rtp_utils.PayloadTypeH264 && s.videoMedia != nil {
		err := s.stream.WritePacketRTP(s.videoMedia, pkt)
		if err != nil {
			logger.Warnf("rtsp", "Video write error: %v (seq=%d, ts=%d, ssrc=%d)",
				err, pkt.SequenceNumber, pkt.Timestamp, pkt.SSRC)
		}
		return err
	}
	if pkt.PayloadType == rtp_utils.PayloadTypeOpus && s.audioMedia != nil {
		err := s.stream.WritePacketRTP(s.audioMedia, pkt)
		if err != nil {
			logger.Warnf("rtsp", "Audio write error: %v", err)
		} else {
			if pkt.SequenceNumber%rtp_utils.LogIntervalPackets == 0 {
				logger.Debugf("rtsp", "Audio packet sent: seq=%d, ts=%d", pkt.SequenceNumber, pkt.Timestamp)
			}
		}
		return err
	}
	logger.Warnf("rtsp", "Unknown payload type: %d", pkt.PayloadType)
	return nil
}

func (s *Server) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	select {
	case <-s.closeChan:
		// already closed
	default:
		close(s.closeChan)
	}

	if s.stream != nil {
		s.stream.Close()
	}
	if s.srv != nil {
		s.srv.Close()
	}
}

func (s *Server) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ctx.Path == "/stream" && s.stream != nil {
		return &base.Response{StatusCode: base.StatusOK}, s.stream, nil
	}
	return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
}

func (s *Server) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ctx.Path == "/stream" && s.stream != nil {
		return &base.Response{StatusCode: base.StatusOK}, s.stream, nil
	}
	return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
}

func (s *Server) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	logger.Debugf("rtsp", "Client started playing: %s", ctx.Path)
	if s.OnPlayCallback != nil {
		s.OnPlayCallback()
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}
