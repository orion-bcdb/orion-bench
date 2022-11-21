package common

import (
	"net/http"
	"regexp"
	"time"

	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var fullQueueExp = regexp.MustCompile(`(?i)transaction queue is full`)

type StatStatus string
type StatOperation string

const (
	Success     StatStatus    = "successful"
	Failed      StatStatus    = "failed"
	FullQueue   StatStatus    = "full_queue"
	Write       StatOperation = "write"
	Read        StatOperation = "read"
	Query       StatOperation = "query"
	AsyncCommit StatOperation = "async_commit"
	SyncCommit  StatOperation = "sync_commit"
)

func GetCommitOp(sync bool) StatOperation {
	if sync {
		return SyncCommit
	} else {
		return AsyncCommit
	}
}

type ClientStats struct {
	lg        *logger.SugarLogger
	registry  *prometheus.Registry
	operation *prometheus.HistogramVec
	mux       *http.ServeMux
}

func (s *ClientStats) Check(err error) {
	utils.Check(s.lg, err)
}

func (s *ClientStats) mustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		s.Check(s.registry.Register(c))
	}
}

func RegisterClientStats(lg *logger.SugarLogger) *ClientStats {
	var LABELS = []string{"status", "operation"}
	s := &ClientStats{
		registry: prometheus.NewRegistry(),
		lg:       lg,
		operation: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "client",
			Name:      "latency_seconds",
			Help:      "The latency (seconds) of an operation",
			Buckets:   utils.TimeBuckets,
		}, LABELS),
		mux: http.NewServeMux(),
	}
	s.mustRegister(
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		prometheus.NewGoCollector(),
		s.operation,
	)
	s.mux.Handle("/metrics", promhttp.InstrumentMetricHandler(
		s.registry, promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}),
	))
	return s
}

func (s *ClientStats) ServePrometheus(addr string) {
	s.lg.Infof("Starting prometheus listner on: %s", addr)
	s.Check(http.ListenAndServe(addr, s.mux))
}

func (s *ClientStats) getStatus(err error) StatStatus {
	if err == nil {
		return Success
	} else if m := fullQueueExp.FindStringSubmatch(err.Error()); m != nil {
		return FullQueue
	} else {
		s.lg.Errorf("WriteTx failed: %s", err)
		return Failed
	}
}

func (s *ClientStats) ObserveOperationLatency(
	operation StatOperation, duration time.Duration, err error,
) {
	s.operation.WithLabelValues(string(s.getStatus(err)), string(operation)).Observe(duration.Seconds())
}

func (s *ClientStats) TimeOperation(
	operation StatOperation, callback func() error,
) error {
	start := time.Now()
	err := callback()
	duration := time.Since(start)
	s.ObserveOperationLatency(operation, duration, err)
	return err
}
