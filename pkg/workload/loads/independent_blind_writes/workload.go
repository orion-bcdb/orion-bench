// Author: Liran Funaro <liran.funaro@ibm.com>

package independent_blind_writes

import (
	"orion-bench/pkg/workload/common"
)

const tableName = "nothing"

type Workload struct {
	common.UserWorkload
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

func (w *Workload) BeforeWork(_ *common.UserWorkloadWorker) {
}

func (w *Workload) Work(userWorkload *common.UserWorkloadWorker) error {
	tx, err := userWorkload.Session.DataTx()
	if err != nil {
		return err
	}
	defer w.CheckAbort(tx)
	err = tx.Put(tableName, userWorkload.UserName, nil, nil)
	if err != nil {
		return err
	}
	return w.BlindCommit(tx)
}
