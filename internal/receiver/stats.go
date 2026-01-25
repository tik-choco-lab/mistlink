package receiver

import (
	"fmt"
	"strings"
	"time"

	"github.com/tik-choco-lab/mistlink/internal/logger"
	"github.com/tik-choco-lab/mistlink/internal/rtp_utils"
)
 
const (
	logInterval       = 5 * time.Second
	gapWarningThreshold = 10
)

type trackStats struct {
	packetCount        int
	lastLogTime        time.Time
	nalTypeStats       map[byte]int
	lastSequenceNumber uint16
	sequenceGaps       int
	firstPacket        bool
}

func newTrackStats() *trackStats {
	return &trackStats{
		packetCount:  0,
		lastLogTime:  time.Now(),
		nalTypeStats: make(map[byte]int),
		firstPacket:  true,
	}
}

func (s *trackStats) checkSequenceGap(currentSeq uint16) []uint16 {
	if s.firstPacket {
		s.lastSequenceNumber = currentSeq
		s.firstPacket = false
		return nil
	}

	diff := int16(currentSeq - s.lastSequenceNumber)
	var missing []uint16
	switch {
	case diff == 1:
		s.lastSequenceNumber = currentSeq
	case diff == 0:
	case diff > 1:
		gap := int(diff) - 1
		s.sequenceGaps += gap
		if gap > 0 {
			for i := uint16(1); i <= uint16(gap); i++ {
				missing = append(missing, s.lastSequenceNumber+i)
			}
			if gap > gapWarningThreshold {
				logger.Warnf("receiver", "Sequence gap: %d → %d (lost: %d)", s.lastSequenceNumber, currentSeq, gap)
			}
		}
		s.lastSequenceNumber = currentSeq
	case diff < 0:
		if s.sequenceGaps > 0 {
			s.sequenceGaps--
		}
	}
	return missing
}

func (s *trackStats) updateNALStats(nalType byte, hasIDR bool) {
	if nalType != 0 {
		s.nalTypeStats[nalType]++
	}
	if hasIDR {
		s.nalTypeStats[rtp_utils.NALTypeIDR]++
	}
}

func (s *trackStats) logIfTime(isVideo bool) {
	if time.Since(s.lastLogTime) <= logInterval {
		return
	}

	if isVideo && len(s.nalTypeStats) > 0 {
		var b strings.Builder
		b.WriteString("NAL stats: ")

		for nalType, count := range s.nalTypeStats {
			name := rtp_utils.GetNALTypeName(nalType)
			b.WriteString(fmt.Sprintf("%s(%d)=%d, ", name, nalType, count))
		}
		statsStr := strings.TrimSuffix(b.String(), ", ")

		expected := s.packetCount + s.sequenceGaps
		if expected > 0 {
			lossRate := float64(s.sequenceGaps) / float64(expected) * 100
			if s.sequenceGaps > 0 {
				statsStr += fmt.Sprintf(" | loss: %d (%.2f%%)", s.sequenceGaps, lossRate)
			}
		}

		logger.Debugf("receiver", "VIDEO %s", statsStr)
		s.nalTypeStats = make(map[byte]int)
		s.sequenceGaps = 0
	} else if !isVideo {
		expected := s.packetCount + s.sequenceGaps
		statsStr := fmt.Sprintf("packets: %d", s.packetCount)
		if expected > 0 {
			lossRate := float64(s.sequenceGaps) / float64(expected) * 100
			if s.sequenceGaps > 0 {
				statsStr += fmt.Sprintf(" | loss: %d (%.2f%%)", s.sequenceGaps, lossRate)
			}
		}
		logger.Debugf("receiver", "AUDIO %s", statsStr)
		s.sequenceGaps = 0
	}

	s.packetCount = 0
	s.lastLogTime = time.Now()
}
