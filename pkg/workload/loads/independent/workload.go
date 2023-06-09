// Author: Liran Funaro <liran.funaro@ibm.com>

package independent

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"
	"orion-bench/pkg/workload/common"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/mroth/weightedrand"
	"github.com/pkg/errors"
)

const tableName = "benchmark_db"

type Workload struct {
	workload *workload.Workload
}

type UserWorkload struct {
	workload      *workload.Workload
	lg            *logger.SugarLogger
	workType      workload.WorkType
	material      *material.BenchMaterial
	userIndex     uint64
	userName      string
	userCrypto    *material.CryptoMaterial
	userSession   bcdb.DBSession
	keyIndex      common.CyclicCounter
	commitCounter common.CyclicCounter
	operations    *weightedrand.Chooser
	signerCount   uint64
	signers       map[string]crypto.Signer
}

type OperationArgs struct {
	name      string
	reads     uint64
	queries   uint64
	writes    uint64
	conflicts uint64
	asserts   uint64
	aclUsers  uint64
	size      uint64
}

type TxParams struct {
	tx           bcdb.TxContext
	readKeys     []string
	assertKeys   []string
	writeKeys    []string
	writeSize    uint64
	writeAcl     *oriontypes.AccessControl
	commit       bool
	sync         bool
	needSign     map[string]crypto.Signer
	readRecords  map[string][]byte
	writeRecords map[string][]byte
	readAcl      map[string]*oriontypes.AccessControl
}

func New(parent *workload.Workload) workload.Worker {
	return &Workload{workload: parent}
}

func (w *Workload) Init() {
	w.workload.CreateTable(tableName)
	w.workload.AddUsers(tableName)
}

func (w *Workload) MakeWorker(userIndex uint64, workType workload.WorkType) workload.UserWorker {
	linesPerUser := uint64(w.workload.GetConfInt("lines-per-user"))
	commitsPerSync := uint64(w.workload.GetConfInt("commits-per-sync"))
	userPos := float64(userIndex) / float64(w.workload.Config.Workload.UserCount)
	// We start the tx counter with an offset to prevent all users to synchronize concurrently
	initialCommitCounter := uint64(userPos * float64(commitsPerSync))

	cfg := w.workload.Config
	if workType == workload.Warmup {
		cfg.Workload.Session.TxTimeout = cfg.Workload.WarmupDuration
	}

	userCrypto := w.workload.Material.User(userIndex)
	worker := &UserWorkload{
		workload:    w.workload,
		lg:          w.workload.Lg,
		workType:    workType,
		material:    w.workload.Material,
		userIndex:   userIndex,
		userName:    userCrypto.Name(),
		userCrypto:  userCrypto,
		userSession: w.workload.Session(userCrypto),
		keyIndex: common.CyclicCounter{
			Value: 0,
			Size:  linesPerUser,
		},
		commitCounter: common.CyclicCounter{
			Value: initialCommitCounter,
			Size:  commitsPerSync,
		},
		signers:     map[string]crypto.Signer{},
		signerCount: 0,
	}

	switch workType {
	case workload.Warmup:
		worker.operations = worker.makeOperationChooser(cfg.Workload.WarmupOperations)
	case workload.Benchmark:
		fallthrough
	default:
		worker.operations = worker.makeOperationChooser(cfg.Workload.Operations)
	}

	// We start from 1 since we don't need the Signer of the current user
	for i := uint64(1); i < worker.signerCount; i++ {
		s := worker.material.User((userIndex + i) % cfg.Workload.UserCount).Signer()
		worker.signers[s.Identity()] = s
	}

	return worker
}

func (w *UserWorkload) Check(err error) {
	utils.Check(w.lg, err)
}

func (w *UserWorkload) parseOperation(operation string) *OperationArgs {
	args := &OperationArgs{
		name: operation,
	}
	op := flag.NewFlagSet(operation, flag.ExitOnError)
	op.Uint64Var(&args.reads, "read", 0, "read X keys")
	op.Uint64Var(&args.queries, "query", 0, "query X keys")
	op.Uint64Var(&args.writes, "write", 0, "write X keys")
	op.Uint64Var(&args.asserts, "assert", 0, "assert X keys")
	op.Uint64Var(&args.conflicts, "conflict", 0, "run X concurrent conflicting reads TXs")
	op.Uint64Var(&args.aclUsers, "acl", 0, "require sig of X users")
	op.Uint64Var(&args.size, "size", 8, "values size")
	w.Check(op.Parse(strings.Split(operation, " ")))
	if args.reads == 0 && args.queries == 0 && args.writes == 0 {
		w.lg.Fatalf("an operation must include reads/writes/query.")
	}
	if (args.reads > 0 || args.writes > 0) && args.queries > 0 {
		w.lg.Fatalf("an operation can only have query or TX, not both.")
	}
	if args.aclUsers > 0 && args.writes == 0 {
		w.lg.Fatalf("an operation must have atleast one write to include ACL.")
	}
	if args.conflicts > 0 && args.writes == 0 {
		w.lg.Fatalf("operation with conflicts must have writes.")
	}
	if args.asserts > 0 && args.reads > 0 {
		w.lg.Fatalf("operation cannot have reads and assert togather.")
	}
	if args.size == 0 {
		args.size = 8
	}

	return args
}

func (w *UserWorkload) makeOperationChooser(ops []types.WorkloadOperation) *weightedrand.Chooser {
	var choices []weightedrand.Choice
	for _, op := range ops {
		if op.Weight == 0 {
			op.Weight = 1
		}
		opArgs := w.parseOperation(op.Operation)
		choices = append(choices, weightedrand.NewChoice(opArgs, op.Weight))
		w.signerCount = common.Max(w.signerCount, opArgs.aclUsers)
	}
	chooser, err := weightedrand.NewChooser(choices...)
	w.Check(err)
	return chooser
}

func (w *UserWorkload) read(tx bcdb.DataTxContext, key string) ([]byte, *oriontypes.Metadata, error) {
	var rawRecord []byte
	var metadata *oriontypes.Metadata
	err := w.workload.Stats.TimeOperation(common.Read, func() (uint64, error) {
		var err error
		rawRecord, metadata, err = tx.Get(tableName, key)
		return 1, err
	})
	if err != nil {
		return nil, nil, err
	}
	return rawRecord, metadata, nil
}

func (w *UserWorkload) write(
	tx bcdb.DataTxContext, key string, value []byte, acl *oriontypes.AccessControl,
) error {
	return w.workload.Stats.TimeOperation(common.Write, func() (uint64, error) {
		return 1, tx.Put(tableName, key, value, acl)
	})
}

func (w *UserWorkload) txCommit(params *TxParams) error {
	if !params.commit {
		return nil
	}

	dataTx, isDataTx := params.tx.(bcdb.DataTxContext)
	if isDataTx && len(params.needSign) > 0 {
		txEnv := w.workload.MultiSignDataTx(dataTx, params.needSign)
		newTx, err := w.userSession.LoadDataTx(txEnv)
		w.Check(err)
		params.tx = newTx
	}

	err := w.workload.Stats.TimeOperation(common.GetCommitOp(params.sync), func() (uint64, error) {
		return 1, w.workload.CommitSync(params.tx, params.sync)
	})
	w.workload.Stats.ObserveContentSize(params.calcTotalWriteSize(), err)

	if err != nil {
		w.commitCounter.Inc(1)
	}

	return err
}

func (w *UserWorkload) txRead(params *TxParams) {
	dataTx, isDataTx := params.tx.(bcdb.DataTxContext)
	if !isDataTx {
		w.lg.Fatal("attempt to read with non data TX")
	}

	for _, k := range params.readKeys {
		record, metadata, err := w.read(dataTx, k)
		if err != nil {
			w.lg.Errorf("failed to read key '%s': %s", k, err)
			continue
		}
		params.readRecords[k] = record
		if metadata != nil {
			params.readAcl[k] = metadata.AccessControl
		}
	}
}

func (w *UserWorkload) txAssert(params *TxParams) {
	dataTx, isDataTx := params.tx.(bcdb.DataTxContext)
	if !isDataTx {
		w.lg.Fatal("attempt to read with non data TX")
	}

	for _, k := range params.assertKeys {
		err := dataTx.AssertRead(tableName, k, &oriontypes.Version{})
		if err != nil {
			w.lg.Errorf("failed to read key '%s': %s", k, err)
		}
	}
}

func (w *UserWorkload) txWrite(params *TxParams) {
	dataTx, isDataTx := params.tx.(bcdb.DataTxContext)
	if !isDataTx {
		w.lg.Fatal("attempt to write with non data TX")
	}

	for _, k := range params.writeKeys {
		value := make([]byte, params.writeSize)
		rand.Read(value)
		err := w.write(dataTx, k, value, params.writeAcl)
		if err != nil {
			w.lg.Errorf("failed to write key '%s': %s", k, err)
			continue
		}
		params.writeRecords[k] = value

		if readAcl := params.readAcl[k]; readAcl != nil {
			for user := range readAcl.ReadWriteUsers {
				if user != w.userName {
					params.needSign[user] = w.signers[user]
				}
			}
		}
	}
}

func (w *UserWorkload) key(line uint64) string {
	return fmt.Sprintf("%s.%d", w.userName, line)
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

func (w *UserWorkload) getAcl(userCount uint64) *oriontypes.AccessControl {
	if userCount == 0 {
		return nil
	}

	acl := &oriontypes.AccessControl{
		ReadWriteUsers:     map[string]bool{w.userName: true},
		SignPolicyForWrite: oriontypes.AccessControl_ALL,
	}

	for k := range w.signers {
		if uint64(len(acl.ReadWriteUsers)) >= userCount {
			break
		}
		acl.ReadWriteUsers[k] = true
	}

	return acl
}

func (w *UserWorkload) query(width uint64) error {
	tx, err := w.userSession.Query()
	w.Check(err)

	return w.workload.Stats.TimeOperation(common.Query, func() (uint64, error) {
		it, err := tx.GetDataByRange(tableName, w.key(w.keyIndex.Value), "", width)
		if err != nil {
			return 0, err
		}
		more := it != nil
		var count uint64 = 0
		for more {
			_, more, err = it.Next()
			if err != nil {
				break
			}
			count += 1
		}

		return count, err
	})
}

func (w *UserWorkload) needCommit(writes uint64) bool {
	return writes > 0
}

func (w *UserWorkload) needSync(writes uint64) bool {
	return w.needCommit(writes) && ((w.workType == workload.Warmup && w.keyIndex.IsNextCompleteCycle(writes)) ||
		(w.commitCounter.Size > 0 && w.commitCounter.Value == 0))
}

func (w *UserWorkload) getTxParams(args *OperationArgs) *TxParams {
	tx, err := w.userSession.DataTx()
	w.Check(err)
	return &TxParams{
		tx:           tx,
		commit:       w.needCommit(args.writes),
		sync:         w.needSync(args.writes),
		readKeys:     w.keyRange(args.reads),
		assertKeys:   w.keyRange(args.asserts),
		writeKeys:    w.keyRange(args.writes),
		writeAcl:     w.getAcl(args.aclUsers),
		writeSize:    args.size,
		needSign:     map[string]crypto.Signer{},
		readRecords:  map[string][]byte{},
		writeRecords: map[string][]byte{},
		readAcl:      map[string]*oriontypes.AccessControl{},
	}
}

func (t *TxParams) calcTotalWriteSize() uint64 {
	var size uint64 = 0
	for k, v := range t.writeRecords {
		size += uint64(len(k))
		size += uint64(len(v))
	}
	return size
}

func (w *UserWorkload) getWriteConflictTxParams(main *TxParams) *TxParams {
	tx, err := w.userSession.DataTx()
	w.Check(err)
	return &TxParams{
		tx:           tx,
		commit:       true,
		sync:         main.sync,
		readKeys:     nil,
		writeKeys:    main.writeKeys,
		writeAcl:     main.writeAcl,
		writeSize:    main.writeSize,
		needSign:     map[string]crypto.Signer{},
		readRecords:  map[string][]byte{},
		writeRecords: map[string][]byte{},
		readAcl:      map[string]*oriontypes.AccessControl{},
	}
}

func (w *UserWorkload) getReadConflictTxParams(main *TxParams) *TxParams {
	tx, err := w.userSession.DataTx()
	w.Check(err)
	return &TxParams{
		tx:           tx,
		commit:       true,
		sync:         main.sync,
		readKeys:     main.writeKeys,
		writeKeys:    nil,
		writeAcl:     nil,
		writeSize:    main.writeSize,
		needSign:     map[string]crypto.Signer{},
		readRecords:  map[string][]byte{},
		writeRecords: map[string][]byte{},
		readAcl:      map[string]*oriontypes.AccessControl{},
	}
}

func (w *UserWorkload) transaction(op *OperationArgs) error {
	mainParams := w.getTxParams(op)
	params := []*TxParams{mainParams}
	for i := uint64(0); i < op.conflicts; i++ {
		p := w.getReadConflictTxParams(mainParams)
		params = append(params, p)
	}

	for _, p := range params {
		//goland:noinspection GoDeferInLoop
		defer w.workload.CheckAbort(p.tx)
	}

	for _, p := range params {
		w.txRead(p)
	}

	for _, p := range params {
		w.txAssert(p)
	}

	for _, p := range params {
		w.txWrite(p)
	}

	var commitErr []error
	for _, p := range params {
		if err := w.txCommit(p); err != nil {
			commitErr = append(commitErr, err)
		}
	}

	if commitErr != nil {
		return errors.Errorf("failed to commit: %s", commitErr)
	}

	return nil
}

func (w *UserWorkload) drawOperation() *OperationArgs {
	return w.operations.Pick().(*OperationArgs)
}

func (w *UserWorkload) Work() workload.WorkStatus {
	op := w.drawOperation()

	var err error
	if op.queries > 0 {
		err = w.query(op.queries)
	} else {
		err = w.transaction(op)
	}

	if err != nil {
		w.lg.Errorf("Op '%s' faild with error: %s", op.name, err)
		return workload.NeedBackoff
	}

	cycleCompleted := w.keyIndex.Inc(common.Max(1, op.writes))
	if w.workType == workload.Warmup && cycleCompleted {
		return workload.Enough
	}
	return workload.Ok
}
