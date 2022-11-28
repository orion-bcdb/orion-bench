// Author: Liran Funaro <liran.funaro@ibm.com>

package workload

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload/common"

	"github.com/cenkalti/backoff"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

type WorkType string

const Warmup WorkType = "warmup"
const Benchmark WorkType = "benchmark"

type Worker interface {
	Init()
	MakeWorker(userIndex uint64) UserWorker
}

type UserWorker interface {
	// Work returns true if a backoff is required
	Work(w WorkType) bool
}

type Workload struct {
	Lg         *logger.SugarLogger
	Config     *types.BenchmarkConf
	Stats      *common.ClientStats
	Material   *material.BenchMaterial
	WorkerRank uint64
	Worker     Worker

	// Evaluated lazily
	db        unsafe.Pointer
	sessions  *sync.Map
	waitInit  *sync.WaitGroup
	waitStart *sync.WaitGroup
	waitEnd   *sync.WaitGroup
	endTime   time.Time
}

type UserParameters struct {
	Index    uint64
	Name     string
	Material *material.BenchMaterial
	Crypto   *material.CryptoMaterial
	Session  bcdb.DBSession
}

func New(
	workerRank uint64, config *types.BenchmarkConf, benchMaterial *material.BenchMaterial, lg *logger.SugarLogger,
) *Workload {
	return &Workload{
		Lg:         lg,
		Config:     config,
		Stats:      common.RegisterClientStats(lg),
		Material:   benchMaterial,
		WorkerRank: workerRank,
		sessions:   &sync.Map{},
	}
}

func (w *Workload) Check(err error) {
	utils.Check(w.Lg, err)
}

func (w *Workload) Replicas() []*sdkconfig.Replica {
	var replicas []*sdkconfig.Replica
	for _, nodeData := range w.Material.AllNodes() {
		//goland:noinspection HttpUrlsUsage
		replicas = append(replicas, &sdkconfig.Replica{
			ID:       nodeData.Crypto.Name(),
			Endpoint: fmt.Sprintf("http://%s:%d", nodeData.Address, nodeData.NodePort),
		})
	}
	return replicas
}

func (w *Workload) DB() bcdb.BCDB {
	dbPtr := atomic.LoadPointer(&w.db)
	if dbPtr != nil {
		return *(*bcdb.BCDB)(dbPtr)
	}
	db, err := bcdb.Create(&sdkconfig.ConnectionConfig{
		ReplicaSet: w.Replicas(),
		RootCAs:    []string{w.Material.RootUser().CertPath()},
		Logger:     w.Lg,
		//TLSConfig:  c.Material().ServerTLS(),
	})
	w.Check(err)
	swapped := atomic.CompareAndSwapPointer(&w.db, nil, unsafe.Pointer(&db))
	if swapped {
		return db
	}
	return *(*bcdb.BCDB)(atomic.LoadPointer(&w.db))
}

func (w *Workload) Session(userCrypto *material.CryptoMaterial) bcdb.DBSession {
	name := userCrypto.Name()
	session, ok := w.sessions.Load(name)
	if ok {
		return session.(bcdb.DBSession)
	}

	session, err := w.DB().Session(&sdkconfig.SessionConfig{
		UserConfig:   userCrypto.Config(),
		TxTimeout:    w.Config.Workload.Session.TxTimeout,
		QueryTimeout: w.Config.Workload.Session.QueryTimeout,
		//ClientTLS:    userCrypto.TLS(),
	})
	w.Check(err)

	actualSession, _ := w.sessions.LoadOrStore(name, session)
	return actualSession.(bcdb.DBSession)
}

func (w *Workload) AdminSession() bcdb.DBSession {
	return w.Session(w.Material.AdminUser())
}

func (w *Workload) UserSession(i uint64) bcdb.DBSession {
	return w.Session(w.Material.User(i))
}

func (w *Workload) CheckAbort(tx bcdb.TxContext) {
	err := tx.Abort()
	if err != nil && err != bcdb.ErrTxSpent {
		w.Check(err)
	}
}

func (w *Workload) CheckCommit(tx bcdb.TxContext) {
	w.Check(w.CommitSync(tx, true))
}

func (w *Workload) CommitSync(tx bcdb.TxContext, sync bool) error {
	txID, receiptEnv, err := tx.Commit(sync)
	if err == nil {
		w.Lg.Debugf("Commited txID: %s, receipt: %+v", txID, receiptEnv.GetResponse().GetReceipt())
	}
	return err
}

func (w *Workload) sign(s crypto.Signer, txEnv *oriontypes.DataTxEnvelope) {
	sig, err := cryptoservice.SignTx(s, txEnv.Payload)
	w.Check(err)
	txEnv.Signatures[s.Identity()] = sig
}

func (w *Workload) MultiSignDataTx(tx bcdb.DataTxContext, signers map[string]crypto.Signer) *oriontypes.DataTxEnvelope {
	msg, err := tx.SignConstructedTxEnvelopeAndCloseTx()
	w.Check(err)
	txEnv := msg.(*oriontypes.DataTxEnvelope)
	for _, s := range signers {
		w.sign(s, txEnv)
	}
	return txEnv
}

func (w *Workload) CreateTable(tableName string, indices ...string) {
	index := make(map[string]oriontypes.IndexAttributeType)
	for _, ind := range indices {
		index[ind] = oriontypes.IndexAttributeType_STRING
	}

	tx, err := w.AdminSession().DBsTx()
	w.Check(err)
	defer w.CheckAbort(tx)
	w.Check(tx.CreateDB(tableName, index))
	w.CheckCommit(tx)
}

func (w *Workload) AddUsers(dbName ...string) {
	commonPrivilege := &oriontypes.Privilege{
		DbPermission: make(map[string]oriontypes.Privilege_Access),
		Admin:        false,
	}
	for _, db := range dbName {
		commonPrivilege.DbPermission[db] = oriontypes.Privilege_ReadWrite
	}

	tx, err := w.AdminSession().UsersTx()
	w.Check(err)
	defer w.CheckAbort(tx)
	for _, user := range w.Material.AllUsers() {
		w.Check(tx.PutUser(&oriontypes.User{
			Id:          user.Name(),
			Certificate: user.Cert().Raw,
			Privilege:   commonPrivilege,
		}, nil))
	}
	w.CheckCommit(tx)
}

func (w *Workload) GetConfString(key string) string {
	return w.Config.Workload.Parameters[key]
}

func (w *Workload) GetConfInt(key string) int {
	intVar, err := strconv.Atoi(w.GetConfString(key))
	w.Check(err)
	return intVar
}

func (w *Workload) GetConfFloat(key string) float64 {
	intVar, err := strconv.ParseFloat(w.GetConfString(key), 64)
	w.Check(err)
	return intVar
}

func (w *Workload) GetConfBool(key string) bool {
	boolVar, err := strconv.ParseBool(w.GetConfString(key))
	w.Check(err)
	return boolVar
}

func (w *Workload) WorkerUsers() []uint64 {
	r := w.WorkerRank
	c := uint64(len(w.Config.Workload.Workers))
	var users []uint64
	for i := r; i < w.Config.Workload.UserCount; i += c {
		users = append(users, i)
	}
	return users
}

func (w *Workload) ServePrometheus() {
	w.Stats.ServePrometheus(w.Material.Worker(w.WorkerRank).PrometheusServeAddress())
}

func (w *Workload) Init() {
	w.Worker.Init()
}

func (w *Workload) RunBenchmark() {
	w.RunAllUsers(Benchmark, w.Config.Workload.Duration)
}

func (w *Workload) RunWarmup() {
	w.RunAllUsers(Warmup, w.Config.Workload.WarmupDuration)
}

func (w *Workload) RunAllUsers(workType WorkType, duration time.Duration) {
	go w.ServePrometheus()

	w.Lg.Infof("Running %s (rank: %d).", workType, w.WorkerRank)

	w.waitInit = &sync.WaitGroup{}
	w.waitStart = &sync.WaitGroup{}
	w.waitEnd = &sync.WaitGroup{}

	users := w.WorkerUsers()
	w.waitInit.Add(len(users))
	w.waitStart.Add(1)

	w.Lg.Infof("Initiating workers (%d users).", len(users))
	for _, userIndex := range users {
		go w.RunUserWork(userIndex, workType)
	}

	w.waitInit.Wait()
	w.Lg.Infof("Workers finished initialization.")

	w.endTime = time.Now().Add(duration)
	w.waitEnd.Add(len(users))

	w.waitStart.Done()
	w.Lg.Infof("Work started.")
	common.WaitTimeout(w.waitEnd, duration+time.Second*10)
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

func (w *Workload) RunUserWork(userIndex uint64, workType WorkType) {
	worker := w.Worker.MakeWorker(userIndex)
	expBackoff := NewExponentialBackOff(&w.Config.Workload.Session.Backoff)
	w.waitInit.Done()

	w.waitStart.Wait()
	for w.endTime.After(time.Now()) {
		needBackoff := worker.Work(workType)
		if !needBackoff {
			expBackoff.Reset()
		} else {
			duration := expBackoff.NextBackOff()
			if duration == backoff.Stop {
				w.Lg.Fatalf("Exponential backoff process stopped")
			}
			w.Stats.ObserveBackoff(duration)
			time.Sleep(duration)
		}
	}
	w.waitEnd.Done()
}
