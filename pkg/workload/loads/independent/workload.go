// Author: Liran Funaro <liran.funaro@ibm.com>

package independent

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"strings"

	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"
	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

const tableName = "benchmark_db"

type TableData struct {
	Counter uint64 `json:"counter"`
}

type Workload struct {
	workload *workload.Workload
	lg       *logger.SugarLogger
}

type UserWorkload struct {
	workload      *workload.Workload
	params        *workload.UserParameters
	keyIndex      common.CyclicCounter
	commitCounter common.CyclicCounter
	cdf           []WorkloadCdf
	acl           *oriontypes.AccessControl
}

type OperationArgs struct {
	readWidth  uint64
	queryWidth uint64
	writeWidth uint64
}

type WorkloadCdf struct {
	cumulativePercent uint32
	operation         OperationArgs
}

type TxParams struct {
	readKeys  []string
	writeKeys []string
	sync      bool
}

func New(parent *workload.Workload) workload.Worker {
	return &Workload{workload: parent, lg: parent.Lg}
}

func (w *Workload) Check(err error) {
	utils.Check(w.lg, err)
}

func (w *Workload) Init() {
	w.workload.CreateTable(tableName)
	w.workload.AddUsers(tableName)
}

func (w *Workload) parseOperation(operation string) OperationArgs {
	args := OperationArgs{}
	op := flag.NewFlagSet(operation, flag.ExitOnError)
	op.Uint64Var(&args.readWidth, "read", 0, "perform read of X keys")
	op.Uint64Var(&args.queryWidth, "query", 0, "perform range query of X keys")
	op.Uint64Var(&args.writeWidth, "write", 0, "perform write of X keys")
	w.Check(op.Parse(strings.Split(operation, " ")))
	if args.readWidth == 0 && args.queryWidth == 0 && args.writeWidth == 0 {
		w.lg.Fatalf("an operation must include reads/writes/query.")
	}
	if (args.readWidth > 0 || args.writeWidth > 0) && args.queryWidth > 0 {
		w.lg.Fatalf("an operation can only have query or TX, not both.")
	}
	return args
}

func (w *Workload) MakeWorker(p *workload.UserParameters) workload.UserWorker {
	linesPerUser := uint64(w.workload.GetConfInt("lines-per-user"))
	commitsPerSync := uint64(w.workload.GetConfInt("commits-per-sync"))
	userPos := float64(p.Index) / float64(w.workload.Config.Workload.UserCount)
	// We start the tx counter with an offset to prevent all users to synchronize concurrently
	initialCommitCounter := uint64(userPos * float64(commitsPerSync))

	worker := &UserWorkload{
		workload: w.workload,
		params:   p,
		keyIndex: common.CyclicCounter{
			Value: 0,
			Size:  linesPerUser,
		},
		commitCounter: common.CyclicCounter{
			Value: initialCommitCounter,
			Size:  commitsPerSync,
		},
	}

	if w.workload.GetConfBool("with-acl") {
		worker.acl = &oriontypes.AccessControl{
			ReadWriteUsers:     map[string]bool{p.Name: true},
			SignPolicyForWrite: oriontypes.AccessControl_ALL,
		}
	}

	var distSum uint32 = 0
	for _, dist := range w.workload.Config.Workload.Distributions {
		if dist.Percent == 0 {
			continue
		}
		distSum += dist.Percent
		worker.cdf = append(worker.cdf, WorkloadCdf{
			cumulativePercent: distSum,
			operation:         w.parseOperation(dist.Operation),
		})
	}
	if distSum != 100 {
		w.lg.Fatalf("Operations does not sum to 100. Current sum: %d.", distSum)
	}

	return worker
}

func (w *UserWorkload) innerTx(tx bcdb.DataTxContext, params *TxParams) error {
	writeRecord := &TableData{Counter: 0}
	for _, k := range params.readKeys {
		var rawRecord []byte
		err := w.workload.Stats.TimeOperation(common.Read, func() error {
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
		w.workload.Check(json.Unmarshal(rawRecord, readRecord))
		writeRecord.Counter += readRecord.Counter
	}

	for _, k := range params.writeKeys {
		writeRecord.Counter += 1
		rawRecord, err := json.Marshal(writeRecord)
		w.workload.Check(err)
		err = w.workload.Stats.TimeOperation(common.Write, func() error {
			return tx.Put(tableName, k, rawRecord, w.acl)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *UserWorkload) key(line uint64) string {
	return fmt.Sprintf("%s.%d", w.params.Name, line)
}

func (w *UserWorkload) keyRange(length uint64) []string {
	var ret []string
	// Copy key index to avoid incrementing the global state
	keyIndex := w.keyIndex
	for i := uint64(0); i < length; i++ {
		ret = append(ret, w.key(keyIndex.Value))
		keyIndex.Inc(1)
	}
	return ret
}

func (w *UserWorkload) getTxParams(readWidth uint64, writeWidth uint64) *TxParams {
	return &TxParams{
		sync:      writeWidth > 0 && w.commitCounter.Size > 0 && w.commitCounter.Value == 0,
		readKeys:  w.keyRange(readWidth),
		writeKeys: w.keyRange(writeWidth),
	}
}

func (w *UserWorkload) incCounter(writeWidth uint64) {
	w.keyIndex.Inc(common.Max(1, writeWidth))
	if writeWidth > 0 {
		w.commitCounter.Inc(1)
	}
}

func (w *UserWorkload) query(width uint64) error {
	tx, err := w.params.Session.Query()
	if err != nil {
		return err
	}

	return w.workload.Stats.TimeOperation(common.Query, func() error {
		_, err := tx.GetDataByRange(tableName, w.key(w.keyIndex.Value), "", width)
		// No need to consume the iterator since the values already fetched
		return err
	})
}

func (w *UserWorkload) transaction(params *TxParams) error {
	tx, err := w.params.Session.DataTx()
	if err != nil {
		return err
	}
	defer w.workload.CheckAbort(tx)
	if err = w.innerTx(tx, params); err != nil {
		return err
	}
	if len(params.writeKeys) == 0 {
		return nil
	}

	return w.workload.Stats.TimeOperation(common.GetCommitOp(params.sync), func() error {
		return w.workload.CommitSync(tx, params.sync)
	})
}

func (w *UserWorkload) drawOperation(workType workload.WorkType) *OperationArgs {
	if workType == workload.Warmup {
		return &OperationArgs{writeWidth: 1, queryWidth: 0, readWidth: 0}
	}
	coin := uint32(rand.Float64() * 100)
	for _, dist := range w.cdf {
		if dist.cumulativePercent > coin {
			return &dist.operation
		}
	}
	return &w.cdf[len(w.cdf)-1].operation
}

func (w *UserWorkload) Work(workType workload.WorkType) error {
	op := w.drawOperation(workType)

	var err error
	if op.queryWidth > 0 {
		err = w.query(op.queryWidth)
	} else {
		err = w.transaction(w.getTxParams(op.readWidth, op.writeWidth))
	}

	if err == nil {
		w.incCounter(op.writeWidth)
	}
	return err
}
