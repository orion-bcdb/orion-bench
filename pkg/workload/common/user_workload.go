// Author: Liran Funaro <liran.funaro@ibm.com>

package common

import (
	"net/http"
	"regexp"
	"sync"
	"time"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/cenkalti/backoff"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Worker interface {
	BeforeWork(w *UserWorkloadWorker)
	Work(w *UserWorkloadWorker) error
}

var fullQueueExp = regexp.MustCompile(`(?i)transaction queue is full`)

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
	Backoff       *backoff.ExponentialBackOff
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
	w.endTime = w.startTime.Add(w.Config.Workload.Duration)
	w.waitEnd.Add(len(users))

	w.waitStart.Done()
	for waitTimeout(&w.waitEnd, w.Config.Workload.LogReportInterval) && w.endTime.After(time.Now()) {
		w.Report()
	}

	w.Report()
}

func NewExponentialBackOff(conf *types.BackoffConf) *backoff.ExponentialBackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     conf.InitialInterval,
		RandomizationFactor: conf.RandomizationFactor,
		Multiplier:          conf.Multiplier,
		MaxInterval:         conf.MaxInterval,
		MaxElapsedTime:      conf.MaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

func (w *UserWorkload) RunUserWorkload(workerIndex uint64, userIndex uint64) {
	crypto := w.material.User(userIndex)
	userWorkload := &UserWorkloadWorker{
		UserIndex:  userIndex,
		UserCrypto: crypto,
		UserName:   crypto.Name(),
		Session:    w.Session(crypto),
	}
	expBackoff := NewExponentialBackOff(&w.Config.Workload.Session.Backoff)

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
			utils.SuccessTx.Observe(latency)
			userWorkload.stats.SuccessCount++
			expBackoff.Reset()
		} else {
			if m := fullQueueExp.FindStringSubmatch(err.Error()); m != nil {
				utils.FullQueueTx.Observe(latency)
			} else {
				w.Lg.Errorf("Tx failed: %s", err)
				utils.FailedTx.Observe(latency)
			}
			userWorkload.stats.FailCount++

			duration := expBackoff.NextBackOff()
			if duration == backoff.Stop {
				w.Lg.Fatalf("Exponential backoff process stopped")
			}
			time.Sleep(duration)
		}
	}

	w.waitEnd.Done()
}
