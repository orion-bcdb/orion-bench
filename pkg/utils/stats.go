package utils

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
)

var timeBuckets = []float64{
	math.Inf(-1), 0,
	1e-9, 1e-8, 1e-7, 1e-6, 1e-5,
	1e-4, 2.5e-4, 5e-4, 7.5e-4,
	1e-3, 2.5e-3, 5e-3, 7.5e-3,
	1e-2, 2.5e-2, 5e-2, 7.5e-2,
	1e-1, 2.5e-1, 5e-1, 7.5e-1,
	1, 2.5, 5, 7.5,
	10, 25, 50, 75,
	1e2, 1e3, 1e4, 1e5, 1e6,
	math.Inf(1),
}

var SuccessTx = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "successful_tx_latency_seconds",
	Help:      "The total number of successful transactions",
	Buckets:   timeBuckets,
})

var FailedTx = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "failed_tx_latency_seconds",
	Help:      "The total number of failed transactions",
	Buckets:   timeBuckets,
})

var FullQueueTx = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "client",
	Name:      "full_queue_tx_latency_seconds",
	Help:      "The total number of failed transactions due to a full queue",
	Buckets:   timeBuckets,
})

var DataSize = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "data",
	Name:      "size_bytes",
	Help:      "The size of the data folder in bytes",
})

func RegisterClient() *prometheus.Registry {
	var r = prometheus.NewRegistry()
	r.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	r.MustRegister(prometheus.NewGoCollector())
	r.MustRegister(SuccessTx)
	r.MustRegister(FailedTx)
	r.MustRegister(FullQueueTx)
	return r
}

func RegisterNode() prometheus.Registerer {
	var r = prometheus.DefaultRegisterer
	r.MustRegister(DataSize)
	return r
}
