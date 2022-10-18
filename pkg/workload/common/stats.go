// Author: Liran Funaro <liran.funaro@ibm.com>

package common

import (
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type Stats struct {
	SuccessCount uint64
	FailCount    uint64
}

type StatsTime struct {
	Stats
	CollectionTime time.Time
}

type StatsDuration struct {
	Stats
	CollectionDuration time.Duration
}

func (s *Stats) AddInPlace(other *Stats) {
	s.SuccessCount += other.SuccessCount
	s.FailCount += other.FailCount
}

func (s *StatsTime) Sub(other *StatsTime) *StatsDuration {
	return &StatsDuration{
		Stats: Stats{
			SuccessCount: s.SuccessCount - other.SuccessCount,
			FailCount:    s.FailCount - other.FailCount,
		},
		CollectionDuration: s.CollectionTime.Sub(other.CollectionTime),
	}
}

func (s *StatsTime) From(t time.Time) *StatsDuration {
	return &StatsDuration{
		Stats: Stats{
			SuccessCount: s.SuccessCount,
			FailCount:    s.FailCount,
		},
		CollectionDuration: s.CollectionTime.Sub(t),
	}
}

func (s *StatsDuration) SuccessPerSecond() float64 {
	return float64(s.SuccessCount) / s.CollectionDuration.Seconds()
}

func (s *StatsDuration) FailPerSecond() float64 {
	return float64(s.FailCount) / s.CollectionDuration.Seconds()
}

func (s *StatsDuration) Report(lg *logger.SugarLogger, title string) {
	p := message.NewPrinter(language.English)
	lg.Infof("[%-10s] TX: [%s] - Duration: %-5s - Tx/sec: %5.3f", title,
		p.Sprintf("Success: %10d Fail: %10d", s.SuccessCount, s.FailCount),
		s.CollectionDuration.Round(time.Millisecond*100), s.SuccessPerSecond())
}
