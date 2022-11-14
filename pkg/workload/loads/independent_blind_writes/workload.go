// Author: Liran Funaro <liran.funaro@ibm.com>

package independent_blind_writes

import (
	"fmt"
	"math"

	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

const tableName = "nothing"

type Workload struct {
	common.UserWorkload
}

type State struct {
	LineCounter  uint64
	LinesPerUser uint64
	TxPerSync    uint64
	TxCounter    uint64
}

func New(m *common.Workload) interface{} {
	ret := &Workload{UserWorkload: common.UserWorkload{Workload: *m}}
	ret.Worker = ret
	return ret
}

func (w *Workload) Init() {
	w.CreateTable(tableName)
	w.AddUsers(tableName)
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

func (w *Workload) writeKeyTx(tx bcdb.DataTxContext, key string) error {
	return tx.Put(tableName, key, nil, nil)
}

func (w *Workload) writeKey(userWorkload *common.UserWorkloadWorker, key string, sync bool) error {
	tx, err := userWorkload.Session.DataTx()
	if err != nil {
		return err
	}
	defer w.CheckAbort(tx)
	if err = w.writeKeyTx(tx, key); err != nil {
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

	userWorkload.WorkloadState = &State{
		LineCounter:  0,
		LinesPerUser: linesPerUser,
		TxPerSync:    txPerSync,
		TxCounter:    initialTxCounter,
	}
}

func (w *Workload) Work(userWorkload *common.UserWorkloadWorker) error {
	key, sync := w.getKey(userWorkload)
	err := w.writeKey(userWorkload, key, sync)
	if err == nil {
		w.incCounter(userWorkload)
	}
	return err
}
