package workload

import (
	"log"

	"orion-bench/pkg/material"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type Workload interface {
	Init()
	Run(worker int)
}

type BenchConfig interface {
	Log() *logger.SugarLogger
	Material() *material.BenchMaterial
	DB() bcdb.BCDB
	UserSession(user string) bcdb.DBSession
	Check(err error)
}

type Builder func(config BenchConfig) Workload

var workloads = map[string]Builder{
	"independent-increment": func(config BenchConfig) Workload {
		return &IndependentIncrement{commonWorkload{config: config}}
	},
}

func BuildWorkload(name string, config BenchConfig) Workload {
	builder, ok := workloads[name]
	if !ok {
		log.Fatalf("Invalid workload: %s", name)
	}
	return builder(config)
}

type commonWorkload struct {
	config BenchConfig
}

func (m *commonWorkload) Check(err error) {
	m.Check(err)
}
