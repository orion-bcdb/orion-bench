package workload

import (
	"log"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

type Workload interface {
	Init()
	Run(worker int)
}

type BenchConfig interface {
	Log() *logger.SugarLogger
	Config() *types.YamlConfig
	Material() *material.BenchMaterial
	UserSession(user string) bcdb.DBSession
}

type Builder func(config BenchConfig) Workload

var workloads = map[string]Builder{
	"independent-increment": func(config BenchConfig) Workload {
		return &IndependentIncrement{commonWorkload{config: config, lg: config.Log()}}
	},
}

func BuildWorkload(config BenchConfig) Workload {
	name := config.Config().WorkloadName
	builder, ok := workloads[name]
	if !ok {
		log.Fatalf("Invalid workload: %s", name)
	}
	return builder(config)
}

type commonWorkload struct {
	lg     *logger.SugarLogger
	config BenchConfig
}

func (m *commonWorkload) Check(err error) {
	utils.Check(m.lg, err)
}

func (m *commonWorkload) Abort(tx bcdb.TxContext) {
	err := tx.Abort()
	if err != nil && err != bcdb.ErrTxSpent {
		m.Check(err)
	}
}

func (m *commonWorkload) Commit(tx bcdb.TxContext) {
	txID, receiptEnv, err := tx.Commit(true)
	m.Check(err)
	m.lg.Infof("Commited txID: %s, receipt: %+v", txID, receiptEnv.GetResponse().GetReceipt())
}

func (m *commonWorkload) CreateTable(tableName string, indices ...string) {
	tx, err := m.config.UserSession(material.Admin).DBsTx()
	m.Check(err)
	defer m.Abort(tx)

	index := make(map[string]oriontypes.IndexAttributeType)
	for _, ind := range indices {
		index[ind] = oriontypes.IndexAttributeType_STRING
	}
	m.Check(tx.CreateDB(tableName, index))
	m.Commit(tx)
}

func (m *commonWorkload) AddUsers(dbName ...string) {
	tx, err := m.config.UserSession(material.Admin).UsersTx()
	m.Check(err)
	defer m.Abort(tx)

	for _, user := range m.config.Material().ListUsers() {
		if user.Name() == material.Admin || user.Name() == material.Root {
			continue
		}
		userRecord := &oriontypes.User{
			Id:          user.Name(),
			Certificate: user.Cert().Raw,
			Privilege: &oriontypes.Privilege{
				DbPermission: make(map[string]oriontypes.Privilege_Access),
				Admin:        false,
			},
		}
		for _, db := range dbName {
			userRecord.Privilege.DbPermission[db] = oriontypes.Privilege_ReadWrite
		}
		err = tx.PutUser(userRecord, nil)
		m.Check(err)
	}

	_, _, err = tx.Commit(true)
	m.Check(err)
}
