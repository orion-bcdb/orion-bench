package utils

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
)

var TimeBuckets = []float64{
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

var SizeBase2Buckets = []float64{
	0, 1 << 3, 1 << 6, 1 << 8, 1 << 9,
	1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18,
	1 << 20, 1 << 22, 1 << 24, 1 << 26, 1 << 28,
	1 << 30, math.Inf(1),
}

var DataSize = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "data",
	Name:      "size_bytes",
	Help:      "The size of the data folder in bytes",
})

func RegisterNode() prometheus.Registerer {
	var r = prometheus.DefaultRegisterer
	r.MustRegister(DataSize)
	return r
}
