// Author: Liran Funaro <liran.funaro@ibm.com>

package common

import (
	"math"
	"net/http"
	"regexp"
	"sync"
	"time"

	"orion-bench/pkg/material"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Worker interface {
	BeforeWork(w *UserWorkloadWorker)
	Work(w *UserWorkloadWorker) error
}

var (
	buckets = []float64{
		math.Inf(-1), 0, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1,
		1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, math.Inf(1),
	}
	successTx = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "client",
		Name:      "successful_tx_latency_seconds",
		Help:      "The total number of successful transactions",
		Buckets:   buckets,
	})
	failedTx = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "client",
		Name:      "failed_tx_latency_seconds",
		Help:      "The total number of failed transactions",
		Buckets:   buckets,
	})
	fullQueueTx = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "client",
		Name:      "full_queue_tx_latency_seconds",
		Help:      "The total number of failed transactions due to a full queue",
		Buckets:   buckets,
	})
	fullQueueExp = regexp.MustCompile(`(?i)transaction queue is full`)
)

type UserWorkload struct {
	Workload
	Worker Worker

	// Internal
	workers         []*UserWorkloadWorker
	waitInit        sync.WaitGroup
	waitStart       sync.WaitGroup
	waitEnd         sync.WaitGroup
	startTime       time.Time
	endTime         time.Time
	aggregatedStats *StatsTime
}

type UserWorkloadWorker struct {
	UserIndex     uint64
	UserName      string
	UserCrypto    *material.CryptoMaterial
	Session       bcdb.DBSession
	WorkloadState interface{}
	stats         Stats
}

func (w *UserWorkload) AggregateWorkerStats(collectionTime time.Time) *StatsTime {
	stats := &StatsTime{CollectionTime: collectionTime}
	for _, r := range w.workers {
		stats.AddInPlace(&r.stats)
	}
	return stats
}

func (w *UserWorkload) Report() {
	beginning := &StatsTime{CollectionTime: w.startTime}
	if w.aggregatedStats == nil {
		w.aggregatedStats = beginning
	}

	stats := w.AggregateWorkerStats(time.Now())
	stats.Sub(beginning).Report(w.Lg, "Total")

	diff := stats.Sub(w.aggregatedStats)
	diff.Report(w.Lg, "Window")
	successTx.Observe(float64(diff.SuccessCount))
	failedTx.Observe(float64(diff.FailCount))
	w.aggregatedStats = stats
}

func (w *UserWorkload) Serve() {
	http.Handle("/metrics", promhttp.Handler())
	w.Check(http.ListenAndServe(w.material.Worker(w.workerRank).PrometheusServeAddress(), nil))
}

func (w *UserWorkload) Run() {
	go w.Serve()

	users := w.WorkerUsers()
	w.waitInit.Add(len(users))
	w.waitStart.Add(1)
	w.workers = make([]*UserWorkloadWorker, len(users))
	for i, userIndex := range users {
		go w.RunUserWorkload(uint64(i), userIndex)
	}

	w.waitInit.Wait()
	w.startTime = time.Now()
	w.endTime = w.startTime.Add(w.config.Workload.Duration)
	w.waitEnd.Add(len(users))

	w.waitStart.Done()
	for waitTimeout(&w.waitEnd, w.config.Workload.LogReportInterval) && w.endTime.After(time.Now()) {
		w.Report()
	}

	w.Report()
}

func (w *UserWorkload) RunUserWorkload(workerIndex uint64, userIndex uint64) {
	crypto := w.material.User(userIndex)
	userWorkload := &UserWorkloadWorker{
		UserIndex:  userIndex,
		UserCrypto: crypto,
		UserName:   crypto.Name(),
		Session:    w.Session(crypto),
	}
	w.workers[workerIndex] = userWorkload
	w.Worker.BeforeWork(userWorkload)
	work := w.Worker.Work
	w.waitInit.Done()

	w.waitStart.Wait()
	for w.endTime.After(time.Now()) {
		start := time.Now()
		err := work(userWorkload)
		latency := time.Since(start).Seconds()
		if err == nil {
			successTx.Observe(latency)
			userWorkload.stats.SuccessCount++
		} else {
			if m := fullQueueExp.FindStringSubmatch(err.Error()); m != nil {
				fullQueueTx.Observe(latency)
			} else {
				w.Lg.Errorf("Tx failed: %s", err)
				failedTx.Observe(latency)
			}
			userWorkload.stats.FailCount++
		}
	}

	w.waitEnd.Done()
}
