// Author: Liran Funaro <liran.funaro@ibm.com>

package independent

import (
	"encoding/json"
	"fmt"
	"math"

	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "benchmark_db"

type TableData struct {
	Counter uint64 `json:"counter"`
}

type TxFunc func(userWorkload *common.UserWorkloadWorker, tx bcdb.DataTxContext, key string) error

type Workload struct {
	common.UserWorkload
	txFunc TxFunc
}

type State struct {
	LineCounter  uint64
	LinesPerUser uint64
	TxPerSync    uint64
	TxCounter    uint64
	Acl          *oriontypes.AccessControl
}

func NewBlindWrite(m *common.Workload) interface{} {
	return NewWithTx(m, WriteKeyBlindTx)
}

func NewUpdate(m *common.Workload) interface{} {
	return NewWithTx(m, WriteKeyUpdateTx)
}

func NewWithTx(m *common.Workload, f TxFunc) interface{} {
	ret := &Workload{
		UserWorkload: common.UserWorkload{Workload: *m},
		txFunc:       f,
	}
	ret.Worker = ret
	return ret
}

func (w *Workload) Init() {
	w.CreateTable(tableName)
	w.AddUsers(tableName)
}

func WriteKeyBlindTx(userWorkload *common.UserWorkloadWorker, tx bcdb.DataTxContext, key string) error {
	return tx.Put(tableName, key, nil, userWorkload.WorkloadState.(*State).Acl)
}

func WriteKeyUpdateTx(userWorkload *common.UserWorkloadWorker, tx bcdb.DataTxContext, key string) error {
	record := &TableData{Counter: 0}
	rawRecord, _, err := tx.Get(tableName, key)
	if err != nil {
		return err
	}
	if rawRecord != nil {
		utils.CheckDefault(json.Unmarshal(rawRecord, record))
	}

	record.Counter += 1
	rawRecord, err = json.Marshal(record)
	utils.CheckDefault(err)

	return tx.Put(tableName, key, rawRecord, userWorkload.WorkloadState.(*State).Acl)
}

func (w *Workload) getKey(userWorkload *common.UserWorkloadWorker) (string, bool) {
	state := userWorkload.WorkloadState.(*State)
	key := fmt.Sprintf("%s.%d", userWorkload.UserName, state.LineCounter)
	return key, state.TxPerSync > 0 && state.TxCounter == 0
}

func (w *Workload) incCounter(userWorkload *common.UserWorkloadWorker) {
	state := userWorkload.WorkloadState.(*State)
	state.LineCounter = (state.LineCounter + 1) % state.LinesPerUser
	if state.TxPerSync > 0 {
		state.TxCounter = (state.TxCounter + 1) % state.TxPerSync
	}
}

func (w *Workload) writeKey(userWorkload *common.UserWorkloadWorker, key string, sync bool) error {
	tx, err := userWorkload.Session.DataTx()
	if err != nil {
		return err
	}
	defer w.CheckAbort(tx)
	if err = w.txFunc(userWorkload, tx, key); err != nil {
		return err
	}
	return w.CommitSync(tx, sync)
}

func (w *Workload) BeforeWork(userWorkload *common.UserWorkloadWorker) {
	linesPerUser := uint64(w.GetConfInt("lines-per-user"))
	txPerSync := uint64(w.GetConfInt("tx-per-sync"))
	userPos := float64(userWorkload.UserIndex) / float64(w.Workload.Config.Workload.UserCount-1)
	// We start the tx counter with an offset to prevent all users to synchronize concurrently
	initialTxCounter := uint64(math.Round(userPos * float64(txPerSync)))

	state := &State{
		LineCounter:  0,
		LinesPerUser: linesPerUser,
		TxPerSync:    txPerSync,
		TxCounter:    initialTxCounter,
	}

	if w.GetConfBool("with-acl") {
		state.Acl = &oriontypes.AccessControl{
			ReadWriteUsers:     map[string]bool{userWorkload.UserName: true},
			SignPolicyForWrite: oriontypes.AccessControl_ALL,
		}
	}

	userWorkload.WorkloadState = state
}

func (w *Workload) Work(userWorkload *common.UserWorkloadWorker) error {
	key, sync := w.getKey(userWorkload)
	err := w.writeKey(userWorkload, key, sync)
	if err == nil {
		w.incCounter(userWorkload)
	}
	return err
}
