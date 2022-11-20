// Author: Liran Funaro <liran.funaro@ibm.com>

package common

import (
	"net/http"
	"sync"
	"time"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"

	"github.com/cenkalti/backoff"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Worker interface {
	BeforeWork(w *UserWorkloadWorker)
	Warmup(w *UserWorkloadWorker) error
	Work(w *UserWorkloadWorker) error
}

type UserWorkload struct {
	Workload
	Worker Worker
	Stats  *ClientStats

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
	http.Handle("/metrics", promhttp.InstrumentMetricHandler(
		w.Stats.registry, promhttp.HandlerFor(w.Stats.registry, promhttp.HandlerOpts{}),
	))
	w.Lg.Infof("Starting prometheus listner.")
	w.Check(http.ListenAndServe(w.material.Worker(w.workerRank).PrometheusServeAddress(), nil))
}

func (w *UserWorkload) Run() {
	w.RunAllUsers("workload", w.RunUserWorkload, w.Config.Workload.Duration)
}

func (w *UserWorkload) RunWarmup() {
	w.RunAllUsers("workload", w.RunUserWarmup, w.Config.Workload.WarmupDuration)
}

func (w *UserWorkload) RunAllUsers(name string, userWorkloadFunc func(workerIndex uint64, userIndex uint64), duration time.Duration) {
	w.Lg.Infof("Running %s (rank: %d).", name, w.workerRank)
	w.Stats = RegisterClientStats(w.Lg)
	go w.ServePrometheus()

	users := w.WorkerUsers()
	w.waitInit.Add(len(users))
	w.waitStart.Add(1)
	w.workers = make([]*UserWorkloadWorker, len(users))

	w.Lg.Infof("Initiating workers (%d users).", len(users))
	for i, userIndex := range users {
		go userWorkloadFunc(uint64(i), userIndex)
	}

	w.waitInit.Wait()
	w.Lg.Infof("Workers finished initialization.")

	w.startTime = time.Now()
	w.endTime = w.startTime.Add(duration)
	w.waitEnd.Add(len(users))

	w.waitStart.Done()
	w.Lg.Infof("Work started.")
	w.waitEnd.Wait()
	w.Lg.Infof("Work ended.")
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

func (w *UserWorkload) InitWorker(workerIndex uint64, userIndex uint64) *UserWorkloadWorker {
	crypto := w.material.User(userIndex)
	userWorkload := &UserWorkloadWorker{
		UserIndex:  userIndex,
		UserCrypto: crypto,
		UserName:   crypto.Name(),
		Backoff:    NewExponentialBackOff(&w.Config.Workload.Session.Backoff),
		Session:    w.Session(crypto),
	}
	w.workers[workerIndex] = userWorkload
	w.Worker.BeforeWork(userWorkload)
	return userWorkload
}

func (w *UserWorkload) RunUserWork(workerIndex uint64, userIndex uint64, work func(w *UserWorkloadWorker) error) {
	userWorkload := w.InitWorker(workerIndex, userIndex)
	w.waitInit.Done()

	w.waitStart.Wait()
	for w.endTime.After(time.Now()) {
		err := work(userWorkload)
		if err == nil {
			userWorkload.Backoff.Reset()
		} else {
			duration := userWorkload.Backoff.NextBackOff()
			if duration == backoff.Stop {
				w.Lg.Fatalf("Exponential backoff process stopped. Last error: %s", err)
			}
			time.Sleep(duration)
		}
	}
	w.waitEnd.Done()
}

func (w *UserWorkload) RunUserWarmup(workerIndex uint64, userIndex uint64) {
	w.RunUserWork(workerIndex, userIndex, w.Worker.Warmup)
}

func (w *UserWorkload) RunUserWorkload(workerIndex uint64, userIndex uint64) {
	w.RunUserWork(workerIndex, userIndex, w.Worker.Work)
}
