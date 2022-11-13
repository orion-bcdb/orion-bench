package utils

import (
	"math"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var buckets = []float64{
	math.Inf(-1), 0, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1,
	1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, math.Inf(1),
}

var SuccessTx = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "successful_tx_latency_seconds",
	Help:      "The total number of successful transactions",
	Buckets:   buckets,
})

var FailedTx = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "failed_tx_latency_seconds",
	Help:      "The total number of failed transactions",
	Buckets:   buckets,
})

var FullQueueTx = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "full_queue_tx_latency_seconds",
	Help:      "The total number of failed transactions due to a full queue",
	Buckets:   buckets,
})

var DataSize = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "data",
	Name:      "size_bytes",
	Help:      "The size of the data folder in bytes",
})

func GetFolderSize(path string) int64 {
	var size int64 = 0
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
