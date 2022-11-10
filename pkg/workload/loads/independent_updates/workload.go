// Author: Liran Funaro <liran.funaro@ibm.com>

package independent_updates

import (
	"encoding/json"

	"orion-bench/pkg/workload/common"

	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "counters"

type Workload struct {
	common.UserWorkload
}

type TableData struct {
	Counter uint64 `json:"counter"`
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

	record := &TableData{Counter: 0}
	rawRecord, _, err := tx.Get(tableName, userWorkload.UserName)
	if err != nil {
		return err
	}
	if rawRecord != nil {
		w.Check(json.Unmarshal(rawRecord, record))
	}

	record.Counter += 1
	rawRecord, err = json.Marshal(record)
	if err != nil {
		return err
	}

	err = tx.Put(tableName, userWorkload.UserName, rawRecord, &oriontypes.AccessControl{
		ReadWriteUsers: map[string]bool{
			userWorkload.UserName: true,
		},
		SignPolicyForWrite: oriontypes.AccessControl_ALL,
	})
	if err != nil {
		return err
	}

	return w.Commit(tx)
}
