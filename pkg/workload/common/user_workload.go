package common

import (
	"net/http"
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
	objectives = map[float64]float64{
		0.99: 0.05,
		0.9:  0.05,
		0.5:  0.05,
		0.1:  0.05,
	}
	successTx = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "client_successful_tx_latency_seconds",
		Help:       "The total number of successful transactions in report interval",
		Objectives: objectives,
	})
	failedTx = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "client_failed_tx_latency_seconds",
		Help:       "The total number of failed transactions in report interval",
		Objectives: objectives,
	})
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
	UserIndex  uint64
	UserName   string
	UserCrypto *material.CryptoMaterial
	Session    bcdb.DBSession
	stats      Stats
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
	stats.Sub(beginning).Report(w.lg, "Total")

	diff := stats.Sub(w.aggregatedStats)
	diff.Report(w.lg, "Window")
	successTx.Observe(float64(diff.SuccessCount))
	failedTx.Observe(float64(diff.FailCount))
	w.aggregatedStats = stats
}

func (w *UserWorkload) Serve() {
	http.Handle("/metrics", promhttp.Handler())
	w.Check(http.ListenAndServe(w.material.Worker(w.workerRank).PrometheusServeAddress(), nil))
}

func (w *UserWorkload) Run() {
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
	go w.Serve()

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
		latency := time.Now().Sub(start).Seconds()
		if err != nil {
			w.lg.Errorf("Tx failed: %s", err)
			successTx.Observe(latency)
			userWorkload.stats.FailCount++
		} else {
			failedTx.Observe(latency)
			userWorkload.stats.SuccessCount++
		}
	}

	w.waitEnd.Done()
}
