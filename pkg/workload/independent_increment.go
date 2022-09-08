package workload

import (
	"encoding/json"
	"sync"

	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "counters"

type IndependentIncrement struct {
	CommonWorkload
}

type TableData struct {
	Counter uint64 `json:"counter"`
}

func (w *IndependentIncrement) Init() {
	w.CreateTable(tableName)
	w.AddUsers(tableName)
}

func (w *IndependentIncrement) workerUsers() []string {
	allUsers := w.material.ListUserNames()
	r := w.workerRank
	c := w.config.Workload.WorkerCount
	var users []string
	for i := r; i < len(allUsers); i += c {
		users = append(users, allUsers[i])
	}
	return users
}

func (w *IndependentIncrement) Run() {
	var wg sync.WaitGroup
	for _, user := range w.workerUsers() {
		wg.Add(1)
		go w.UserRun(&wg, user)
	}
	wg.Wait()
}

func (w *IndependentIncrement) UserRun(wg *sync.WaitGroup, user string) {
	w.lg.Infof("Starting user: %s", user)
	for i := 0; i < 1e6; i++ {
		w.UserInc(user)
	}
	wg.Done()
}

func (w *IndependentIncrement) UserInc(user string) {
	tx, err := w.UserSession(user).DataTx()
	w.Check(err)
	defer w.Abort(tx)

	record := &TableData{Counter: 0}
	rawRecord, _, err := tx.Get(tableName, user)
	w.Check(err)
	if rawRecord != nil {
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
