package common

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

type Workload struct {
	lg         *logger.SugarLogger
	config     *types.BenchmarkConf
	material   *material.BenchMaterial
	workerRank uint64

	// Evaluated lazily
	db       unsafe.Pointer
	sessions *sync.Map
}

func New(workerRank uint64, config *types.BenchmarkConf, benchMaterial *material.BenchMaterial, lg *logger.SugarLogger) Workload {
	return Workload{
		lg:         lg,
		config:     config,
		material:   benchMaterial,
		workerRank: workerRank,
		sessions:   &sync.Map{},
	}
}

func (w *Workload) Check(err error) {
	utils.Check(w.lg, err)
}

func (w *Workload) Replicas() []*sdkconfig.Replica {
	var replicas []*sdkconfig.Replica
	for _, nodeData := range w.material.AllNodes() {
		replicas = append(replicas, &sdkconfig.Replica{
			ID:       nodeData.Crypto.Name(),
			Endpoint: "http://" + nodeData.Address + ":" + strconv.Itoa(int(nodeData.NodePort)),
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
		RootCAs:    []string{w.material.RootUser().CertPath()},
		Logger:     w.lg,
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
		TxTimeout:    time.Duration(w.config.Workload.Session.TxTimeout) * time.Second,
		QueryTimeout: time.Duration(w.config.Workload.Session.QueryTimeout) * time.Second,
		//ClientTLS:    userCrypto.TLS(),
	})
	w.Check(err)

	actualSession, _ := w.sessions.LoadOrStore(name, session)
	return actualSession.(bcdb.DBSession)
}

func (w *Workload) AdminSession() bcdb.DBSession {
	return w.Session(w.material.AdminUser())
}

func (w *Workload) UserSession(i uint64) bcdb.DBSession {
	return w.Session(w.material.User(i))
}

func (w *Workload) CheckAbort(tx bcdb.TxContext) {
	err := tx.Abort()
	if err != nil && err != bcdb.ErrTxSpent {
		w.Check(err)
	}
}

func (w *Workload) CheckCommit(tx bcdb.TxContext) {
	w.Check(w.Commit(tx))
}

func (w *Workload) Commit(tx bcdb.TxContext) error {
	txID, receiptEnv, err := tx.Commit(true)
	if err == nil {
		w.lg.Debugf("Commited txID: %s, receipt: %+v", txID, receiptEnv.GetResponse().GetReceipt())
	}
	return err
}

func (w *Workload) BlindCommit(tx bcdb.TxContext) error {
	txID, receiptEnv, err := tx.Commit(false)
	if err == nil {
		w.lg.Debugf("Commited txID: %s, receipt: %+v", txID, receiptEnv.GetResponse().GetReceipt())
	}
	return err
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
	tx, err := w.AdminSession().UsersTx()
	w.Check(err)
	defer w.CheckAbort(tx)

	commonPrivilege := &oriontypes.Privilege{
		DbPermission: make(map[string]oriontypes.Privilege_Access),
		Admin:        false,
	}
	for _, db := range dbName {
		commonPrivilege.DbPermission[db] = oriontypes.Privilege_ReadWrite
	}

	for _, user := range w.material.AllUsers() {
		w.Check(tx.PutUser(&oriontypes.User{
			Id:          user.Name(),
			Certificate: user.Cert().Raw,
			Privilege:   commonPrivilege,
		}, nil))
	}
	w.CheckCommit(tx)
}

func (w *Workload) GetConfString(key string) string {
	return w.config.Workload.Parameters[key]
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

func (w *Workload) WorkerUsers() []uint64 {
	r := w.workerRank
	c := uint64(len(w.config.Workload.Workers))
	var users []uint64
	for i := r; i < w.config.Workload.UserCount; i += c {
		users = append(users, i)
	}
	return users
}
