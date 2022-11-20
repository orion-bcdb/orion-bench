// Author: Liran Funaro <liran.funaro@ibm.com>

package independent

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strings"

	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "benchmark_db"

type TableData struct {
	Counter uint64 `json:"counter"`
}

type Workload struct {
	common.UserWorkload
}

type OperationArgs struct {
	ReadWidth  uint
	QueryWidth uint
	Write      bool
}

type WorkloadCdf struct {
	CumulativePercent uint32
	Operation         OperationArgs
}

type State struct {
	LineCounter  uint64
	LinesPerUser uint64
	TxPerSync    uint64
	TxCounter    uint64
	Cdf          []WorkloadCdf
	Acl          *oriontypes.AccessControl
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

func (w *Workload) InnerTx(userWorkload *common.UserWorkloadWorker, tx bcdb.DataTxContext, params *TxParams) error {
	writeRecord := &TableData{Counter: 0}
	for _, k := range params.readKeys {
		var rawRecord []byte
		err := w.Stats.TimeOperation(common.Read, func() error {
			var err error
			rawRecord, _, err = tx.Get(tableName, k)
			return err
		})
		if err != nil {
			return err
		}
		if rawRecord == nil {
			continue
		}

		readRecord := &TableData{Counter: 0}
		w.Check(json.Unmarshal(rawRecord, readRecord))
		writeRecord.Counter += readRecord.Counter
	}

	if params.writeKey == "" {
		return nil
	}

	writeRecord.Counter += 1
	rawRecord, err := json.Marshal(writeRecord)
	w.Check(err)
	return w.Stats.TimeOperation(common.Write, func() error {
		return tx.Put(tableName, params.writeKey, rawRecord, w.getState(userWorkload).Acl)
	})
}

func (w *Workload) getState(userWorkload *common.UserWorkloadWorker) *State {
	return userWorkload.WorkloadState.(*State)
}

func (w *Workload) key(userWorkload *common.UserWorkloadWorker, line uint64) string {
	return fmt.Sprintf("%s.%d", userWorkload.UserName, line)
}

type TxParams struct {
	readKeys []string
	writeKey string
	sync     bool
}

func nextEntry(curEntry uint64, entryCount uint64) uint64 {
	return (curEntry + 1) % entryCount
}

func (w *Workload) getTxKeyRange(userWorkload *common.UserWorkloadWorker, readWidth uint, write bool) *TxParams {
	state := w.getState(userWorkload)
	params := &TxParams{
		sync: write && state.TxPerSync > 0 && state.TxCounter == 0,
	}

	curLine := state.LineCounter
	if write {
		params.writeKey = w.key(userWorkload, curLine)
	}

	for i := uint(0); i < readWidth; i++ {
		params.readKeys = append(params.readKeys, w.key(userWorkload, curLine))
		curLine = nextEntry(curLine, state.LinesPerUser)
	}

	return params
}

func (w *Workload) incCounter(userWorkload *common.UserWorkloadWorker, write bool) {
	state := w.getState(userWorkload)
	state.LineCounter = nextEntry(state.LineCounter, state.LinesPerUser)
	if state.TxPerSync > 0 && write {
		state.TxCounter = nextEntry(state.TxCounter, state.TxPerSync)
	}
}

func (w *Workload) query(userWorkload *common.UserWorkloadWorker, width uint) error {
	tx, err := userWorkload.Session.Query()
	if err != nil {
		return err
	}

	state := w.getState(userWorkload)
	startKey := w.key(userWorkload, nextEntry(state.LineCounter, state.LinesPerUser))
	return w.Stats.TimeOperation(common.Query, func() error {
		_, err := tx.GetDataByRange(tableName, startKey, "", uint64(width))
		// No need to consume the iterator since the values already fetched
		return err
	})
}

func (w *Workload) transaction(userWorkload *common.UserWorkloadWorker, params *TxParams) error {
	tx, err := userWorkload.Session.DataTx()
	if err != nil {
		return err
	}
	defer w.CheckAbort(tx)
	if err = w.InnerTx(userWorkload, tx, params); err != nil {
		return err
	}
	if params.writeKey == "" {
		return nil
	}

	return w.Stats.TimeOperation(common.GetCommitOp(params.sync), func() error {
		return w.CommitSync(tx, params.sync)
	})
}

func (w *Workload) parseOperation(operation string) OperationArgs {
	args := OperationArgs{}
	op := flag.NewFlagSet(operation, flag.ExitOnError)
	op.UintVar(&args.ReadWidth, "read", 0, "perform read of X keys")
	op.UintVar(&args.QueryWidth, "query", 0, "perform range query of X keys")
	op.BoolVar(&args.Write, "write", false, "perform write")
	w.Check(op.Parse(strings.Split(operation, " ")))
	if args.ReadWidth == 0 && args.QueryWidth == 0 && !args.Write {
		w.Lg.Fatalf("an operation must include reads/writes/query.")
	}
	if (args.ReadWidth > 0 || args.Write) && args.QueryWidth > 0 {
		w.Lg.Fatalf("a work can only have query or TX, not both.")
	}
	return args
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

	var distSum uint32 = 0
	for _, dist := range w.Workload.Config.Workload.Distributions {
		if dist.Percent == 0 {
			continue
		}
		distSum += dist.Percent
		state.Cdf = append(state.Cdf, WorkloadCdf{
			CumulativePercent: distSum,
			Operation:         w.parseOperation(dist.Operation),
		})
	}
	if distSum != 100 {
		w.Lg.Fatalf("Operations does not sum to 100. Current sum: %d.", distSum)
	}

	userWorkload.WorkloadState = state
}

func (w *Workload) drawOperation(userWorkload *common.UserWorkloadWorker) *OperationArgs {
	state := w.getState(userWorkload)
	coin := uint32(rand.Float64() * 100)
	for _, dist := range state.Cdf {
		if dist.CumulativePercent > coin {
			return &dist.Operation
		}
	}
	return &state.Cdf[len(state.Cdf)-1].Operation
}

func (w *Workload) WorkQuery(userWorkload *common.UserWorkloadWorker, op *OperationArgs) error {
	return w.query(userWorkload, op.QueryWidth)
}

func (w *Workload) WorkTx(userWorkload *common.UserWorkloadWorker, op *OperationArgs) error {
	params := w.getTxKeyRange(userWorkload, op.ReadWidth, op.Write)
	return w.transaction(userWorkload, params)
}

func (w *Workload) Warmup(userWorkload *common.UserWorkloadWorker) error {
	op := &OperationArgs{Write: true, QueryWidth: 0, ReadWidth: 0}
	err := w.WorkTx(userWorkload, op)
	if err == nil {
		w.incCounter(userWorkload, op.Write)
	}
	return err
}

func (w *Workload) Work(userWorkload *common.UserWorkloadWorker) error {
	op := w.drawOperation(userWorkload)

	var err error
	if op.QueryWidth > 0 {
		err = w.WorkQuery(userWorkload, op)
	} else {
		err = w.WorkTx(userWorkload, op)
	}

	if err == nil {
		w.incCounter(userWorkload, op.Write)
	}
	return err
}
