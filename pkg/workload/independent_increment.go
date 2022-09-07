package workload

import (
	"encoding/json"
	"sync"

	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "counters"

type IndependentIncrement struct {
	commonWorkload
}

type TableData struct {
	Counter uint64 `json:"counter"`
}

func (w *IndependentIncrement) Init() {
	w.CreateTable(tableName)
	w.AddUsers(tableName)
}

func (w *IndependentIncrement) UserInc(user string) {
	tx, err := w.config.UserSession(user).DataTx()
	w.Check(err)
	defer w.Abort(tx)

	record := &TableData{}
	rawRecord, _, err := tx.Get(tableName, user)
	w.Check(err)
	if rawRecord == nil {
		record.Counter = 0
	} else {
		w.Check(json.Unmarshal(rawRecord, record))
	}
	record.Counter += 1
	w.Check(tx.Put(tableName, user, rawRecord, &oriontypes.AccessControl{
		ReadWriteUsers: map[string]bool{
			user: true,
		},
		SignPolicyForWrite: oriontypes.AccessControl_ALL,
	}))
	w.Commit(tx)
}

func (w *IndependentIncrement) UserRun(wg *sync.WaitGroup, user string) {
	w.lg.Infof("Starting user: %s", user)
	for i := 0; i < 1e6; i++ {
		w.UserInc(user)
	}
	wg.Done()
}

func (w *IndependentIncrement) Run(_ int) {
	var wg sync.WaitGroup
	for _, user := range w.config.Material().ListUserNames() {
		wg.Add(1)
		go w.UserRun(&wg, user)
	}
	wg.Wait()
}
