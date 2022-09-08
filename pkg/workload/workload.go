package workload

import (
	"log"
	"strconv"
	"time"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

type Workload interface {
	Init()
	Run()
}

var workloads = map[string]func(m *CommonWorkload) Workload{
	"independent-increment": func(m *CommonWorkload) Workload {
		return &IndependentIncrement{*m}
	},
}

type CommonWorkload struct {
	lg         *logger.SugarLogger
	config     *types.YamlConfig
	material   *material.BenchMaterial
	workerRank int

	// Evaluated lazily
	db       bcdb.BCDB
	sessions map[string]bcdb.DBSession
}

func New(workerRank int, config *types.YamlConfig, material *material.BenchMaterial, lg *logger.SugarLogger) Workload {
	workload := CommonWorkload{
		lg:         lg,
		config:     config,
		material:   material,
		workerRank: workerRank,
	}
	name := config.Workload.Name
	builder, ok := workloads[name]
	if !ok {
		log.Fatalf("Invalid workload: %s", name)
	}
	return builder(&workload)
}

func (w *CommonWorkload) Check(err error) {
	utils.Check(w.lg, err)
}

func (w *CommonWorkload) Replicas() []*sdkconfig.Replica {
	var replicas []*sdkconfig.Replica
	for serverId, s := range w.config.Cluster {
		replicas = append(replicas, &sdkconfig.Replica{
			ID:       serverId,
			Endpoint: "http://" + s.Address + ":" + strconv.Itoa(int(s.NodePort)),
		})
	}
	return replicas
}

func (w *CommonWorkload) DB() bcdb.BCDB {
	if w.db != nil {
		return w.db
	}

	db, err := bcdb.Create(&sdkconfig.ConnectionConfig{
		ReplicaSet: w.Replicas(),
		RootCAs:    []string{w.material.User(material.Root).CertPath()},
		Logger:     w.lg,
		//TLSConfig:  c.Material().ServerTLS(),
	})
	w.Check(err)
	w.db = db
	return w.db
}

func (w *CommonWorkload) UserSession(user string) bcdb.DBSession {
	if w.sessions == nil {
		w.sessions = make(map[string]bcdb.DBSession)
	}

	session, ok := w.sessions[user]
	if ok {
		return session
	}

	userCrypto := w.material.User(user)
	session, err := w.DB().Session(&sdkconfig.SessionConfig{
		UserConfig:   userCrypto.Config(),
		TxTimeout:    time.Duration(w.config.Session.TxTimeout) * time.Second,
		QueryTimeout: time.Duration(w.config.Session.QueryTimeout) * time.Second,
		//ClientTLS:    userCrypto.TLS(),
	})
	w.Check(err)

	w.sessions[user] = session
	return session
}

func (w *CommonWorkload) Abort(tx bcdb.TxContext) {
	err := tx.Abort()
	if err != nil && err != bcdb.ErrTxSpent {
		w.Check(err)
	}
}

func (w *CommonWorkload) Commit(tx bcdb.TxContext) {
	txID, receiptEnv, err := tx.Commit(true)
	w.Check(err)
	w.lg.Infof("Commited txID: %s, receipt: %+v", txID, receiptEnv.GetResponse().GetReceipt())
}

func (w *CommonWorkload) CreateTable(tableName string, indices ...string) {
	tx, err := w.UserSession(material.Admin).DBsTx()
	w.Check(err)
	defer w.Abort(tx)

	index := make(map[string]oriontypes.IndexAttributeType)
	for _, ind := range indices {
		index[ind] = oriontypes.IndexAttributeType_STRING
	}
	w.Check(tx.CreateDB(tableName, index))
	w.Commit(tx)
}

func (w *CommonWorkload) AddUsers(dbName ...string) {
	commonPrivilege := &oriontypes.Privilege{
		DbPermission: make(map[string]oriontypes.Privilege_Access),
		Admin:        false,
	}
	for _, db := range dbName {
		commonPrivilege.DbPermission[db] = oriontypes.Privilege_ReadWrite
	}

	tx, err := w.UserSession(material.Admin).UsersTx()
	w.Check(err)
	defer w.Abort(tx)
	for _, user := range w.material.ListUsers() {
		w.Check(tx.PutUser(&oriontypes.User{
			Id:          user.Name(),
			Certificate: user.Cert().Raw,
			Privilege:   commonPrivilege,
		}, nil))
	}
	w.Commit(tx)
}
