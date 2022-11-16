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
	workers   []*UserWorkloadWorker
	waitInit  sync.WaitGroup
	waitStart sync.WaitGroup
	waitEnd   sync.WaitGroup
	startTime time.Time
	endTime   time.Time
}

type UserWorkloadWorker struct {
	UserIndex     uint64
	UserName      string
	UserCrypto    *material.CryptoMaterial
	Backoff       *backoff.ExponentialBackOff
	Session       bcdb.DBSession
	WorkloadState interface{}
}

func (w *UserWorkload) ServePrometheus() {
	r := utils.RegisterClient()
	http.Handle("/metrics", promhttp.InstrumentMetricHandler(
		r, promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
	))
	w.Lg.Infof("Starting prometheus listner.")
	w.Check(http.ListenAndServe(w.material.Worker(w.workerRank).PrometheusServeAddress(), nil))
}

func (w *UserWorkload) Run() {
	w.Lg.Infof("Running workload (rank: %d).", w.workerRank)
	go w.ServePrometheus()

	users := w.WorkerUsers()
	w.waitInit.Add(len(users))
	w.waitStart.Add(1)
	w.workers = make([]*UserWorkloadWorker, len(users))

	w.Lg.Infof("Initiating workers (%d users).", len(users))
	for i, userIndex := range users {
		go w.RunUserWorkload(uint64(i), userIndex)
	}

	w.waitInit.Wait()
	w.Lg.Infof("Workers finished initialization.")

	w.startTime = time.Now()
	w.endTime = w.startTime.Add(w.Config.Workload.Duration)
	w.waitEnd.Add(len(users))

	w.waitStart.Done()
	w.Lg.Infof("Workload started.")
	w.waitEnd.Wait()
	w.Lg.Infof("Workload ended.")
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
			expBackoff.Reset()
		} else {
			if m := fullQueueExp.FindStringSubmatch(err.Error()); m != nil {
				utils.FullQueueTx.Observe(latency)
			} else {
				w.Lg.Errorf("Tx failed: %s", err)
				utils.FailedTx.Observe(latency)
			}

			duration := expBackoff.NextBackOff()
			if duration == backoff.Stop {
				w.Lg.Fatalf("Exponential backoff process stopped")
			}
			time.Sleep(duration)
		}
	}

	w.waitEnd.Done()
}
