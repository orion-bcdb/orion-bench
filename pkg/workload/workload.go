// Author: Liran Funaro <liran.funaro@ibm.com>

package workload

import (
	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/workload/common"
	"orion-bench/pkg/workload/loads/independent_blind_writes"
	"orion-bench/pkg/workload/loads/independent_updates"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type Runner interface {
	Init()
	Run()
}

var workloads = map[string]func(m *common.Workload) interface{}{
	"independent-updates":      independent_updates.New,
	"independent-blind-writes": independent_blind_writes.New,
}

func New(workerRank uint64, config *types.BenchmarkConf, material *material.BenchMaterial, lg *logger.SugarLogger) Runner {
	workload := common.New(workerRank, config, material, lg)
	builder, ok := workloads[config.Workload.Name]
	if !ok {
		lg.Fatalf("Invalid workload: %s", config.Workload.Name)
	}
	return builder(&workload).(Runner)
}
