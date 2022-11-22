// Author: Liran Funaro <liran.funaro@ibm.com>

package independent

import (
	"flag"
	"fmt"
	"strings"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"
	"orion-bench/pkg/workload/common"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/mroth/weightedrand"
)

const tableName = "benchmark_db"

type Workload struct {
	workload *workload.Workload
}

type UserWorkload struct {
	workload         *workload.Workload
	lg               *logger.SugarLogger
	material         *material.BenchMaterial
	userIndex        uint64
	userName         string
	userCrypto       *material.CryptoMaterial
	userSession      bcdb.DBSession
	keyIndex         common.CyclicCounter
	commitCounter    common.CyclicCounter
	operations       *weightedrand.Chooser
	warmupOperations *weightedrand.Chooser
	signerCount      uint64
	signers          map[string]crypto.Signer
}

type OperationArgs struct {
	reads    uint64
	queries  uint64
	writes   uint64
	aclUsers uint64
}

type WorkloadWightedOperation struct {
	cumulativeWeight uint64
	operation        OperationArgs
}

type TxParams struct {
	readKeys  []string
	writeKeys []string
	acl       *oriontypes.AccessControl
	sync      bool
	needSign  []crypto.Signer
}

func New(parent *workload.Workload) workload.Worker {
	return &Workload{workload: parent}
}

func (w *Workload) Init() {
	w.workload.CreateTable(tableName)
	w.workload.AddUsers(tableName)
}

func (w *Workload) MakeWorker(userIndex uint64) workload.UserWorker {
	linesPerUser := uint64(w.workload.GetConfInt("lines-per-user"))
	commitsPerSync := uint64(w.workload.GetConfInt("commits-per-sync"))
	userPos := float64(userIndex) / float64(w.workload.Config.Workload.UserCount)
	// We start the tx counter with an offset to prevent all users to synchronize concurrently
	initialCommitCounter := uint64(userPos * float64(commitsPerSync))

	userCrypto := w.workload.Material.User(userIndex)
	worker := &UserWorkload{
		workload:    w.workload,
		lg:          w.workload.Lg,
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

	worker.operations = worker.makeOperationChooser(w.workload.Config.Workload.Operations)
	worker.warmupOperations = worker.makeOperationChooser(w.workload.Config.Workload.WarmupOperations)

	// We start from 1 since we don't need the Signer of the current user
	for i := uint64(1); i < worker.signerCount; i++ {
		s := worker.material.User((userIndex + i) % w.workload.Config.Workload.UserCount).Signer()
		worker.signers[s.Identity()] = s
	}

	return worker
}

func (w *UserWorkload) Check(err error) {
	utils.Check(w.lg, err)
}

func (w *UserWorkload) parseOperation(operation string) *OperationArgs {
	args := &OperationArgs{}
	op := flag.NewFlagSet(operation, flag.ExitOnError)
	op.Uint64Var(&args.reads, "read", 0, "read X keys")
	op.Uint64Var(&args.queries, "query", 0, "query X keys")
	op.Uint64Var(&args.writes, "write", 0, "write X keys")
	op.Uint64Var(&args.aclUsers, "acl", 0, "require sig of X users")
	w.Check(op.Parse(strings.Split(operation, " ")))
	if args.reads == 0 && args.queries == 0 && args.writes == 0 {
		w.lg.Fatalf("an operation must include reads/writes/query.")
	}
	if (args.reads > 0 || args.writes > 0) && args.queries > 0 {
		w.lg.Fatalf("an operation can only have query or TX, not both.")
	}
	if args.aclUsers > 0 && args.writes == 0 {
		w.lg.Fatalf("an operation can must have atleast one write to include ACL.")
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

func (w *UserWorkload) read(tx bcdb.DataTxContext, key string) (*types.TableData, *oriontypes.Metadata, error) {
	var rawRecord []byte
	var metadata *oriontypes.Metadata
	err := w.workload.Stats.TimeOperation(common.Read, func() error {
		var err error
		rawRecord, metadata, err = tx.Get(tableName, key)
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	readRecord := &types.TableData{}
	if rawRecord != nil {
		w.workload.Check(proto.Unmarshal(rawRecord, readRecord))
	}
	return readRecord, metadata, nil
}

func (w *UserWorkload) write(
	tx bcdb.DataTxContext, key string, value *types.TableData, acl *oriontypes.AccessControl,
) error {
	var rawRecord []byte
	if value != nil {
		raw, err := proto.Marshal(value)
		w.workload.Check(err)
		rawRecord = raw
	}
	return w.workload.Stats.TimeOperation(common.Write, func() error {
		return tx.Put(tableName, key, rawRecord, acl)
	})
}

func (w *UserWorkload) commit(tx bcdb.TxContext, params *TxParams) error {
	dataTx, isDataTx := tx.(bcdb.DataTxContext)
	if isDataTx && len(params.needSign) > 0 {
		txEnv, err := w.workload.MultiSignDataTx(dataTx, params.needSign)
		if err != nil {
			return err
		}
		tx, err = w.userSession.LoadDataTx(txEnv)
		if err != nil {
			return err
		}
	}

	return w.workload.Stats.TimeOperation(common.GetCommitOp(params.sync), func() error {
		return w.workload.CommitSync(tx, params.sync)
	})
}

func (w *UserWorkload) innerTx(tx bcdb.DataTxContext, params *TxParams) error {
	records := map[string]*types.TableData{}
	acl := map[string]*oriontypes.AccessControl{}
	for _, k := range params.readKeys {
		record, metadata, err := w.read(tx, k)
		if err != nil {
			return err
		}
		record.Counter += 1
		records[k] = record
		if metadata != nil {
			acl[k] = metadata.AccessControl
		}
	}

	for _, k := range params.writeKeys {
		record, _ := records[k]
		if err := w.write(tx, k, record, params.acl); err != nil {
			return err
		}

		prevAcl, _ := acl[k]
		if prevAcl != nil {
			for user := range prevAcl.ReadWriteUsers {
				if user == w.userName {
					continue
				}
				params.needSign = append(params.needSign, w.signers[user])
			}
		}
	}

	return nil
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

func (w *UserWorkload) getTxParams(args *OperationArgs) *TxParams {
	return &TxParams{
		sync:      args.writes > 0 && w.commitCounter.Size > 0 && w.commitCounter.Value == 0,
		readKeys:  w.keyRange(args.reads),
		writeKeys: w.keyRange(args.writes),
		acl:       w.getAcl(args.aclUsers),
	}
}

func (w *UserWorkload) incCounter(writeWidth uint64) {
	w.keyIndex.Inc(common.Max(1, writeWidth))
	if writeWidth > 0 {
		w.commitCounter.Inc(1)
	}
}

func (w *UserWorkload) query(width uint64) error {
	tx, err := w.userSession.Query()
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
	tx, err := w.userSession.DataTx()
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
	return w.commit(tx, params)
}

func (w *UserWorkload) drawOperation(workType workload.WorkType) *OperationArgs {
	var chooser *weightedrand.Chooser
	switch workType {
	case workload.Warmup:
		chooser = w.warmupOperations
	case workload.Benchmark:
		fallthrough
	default:
		chooser = w.operations
	}
	return chooser.Pick().(*OperationArgs)
}

func (w *UserWorkload) Work(workType workload.WorkType) error {
	op := w.drawOperation(workType)

	var err error
	if op.queries > 0 {
		err = w.query(op.queries)
	} else {
		err = w.transaction(w.getTxParams(op))
	}

	if err == nil {
		w.incCounter(op.writes)
	}
	return err
}
