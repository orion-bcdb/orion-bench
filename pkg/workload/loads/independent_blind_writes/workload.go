// Author: Liran Funaro <liran.funaro@ibm.com>

package independent_blind_writes

import (
	"fmt"

	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

const tableName = "nothing"

type Workload struct {
	common.UserWorkload
}

type State struct {
	Counter      uint64
	LinesPerUser uint64
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

func (w *Workload) nextKey(userWorkload *common.UserWorkloadWorker) (string, uint64) {
	state := userWorkload.WorkloadState.(*State)
	key := fmt.Sprintf("%s.%d", userWorkload.UserName, state.Counter)
	state.Counter = (state.Counter + 1) % state.LinesPerUser
	return key, state.Counter
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
	userWorkload.WorkloadState = &State{
		Counter:      0,
		LinesPerUser: uint64(w.GetConfInt("lines-per-user")),
	}
}

func (w *Workload) Work(userWorkload *common.UserWorkloadWorker) error {
	key, nextCount := w.nextKey(userWorkload)
	return w.writeKey(userWorkload, key, nextCount == 0)
}
