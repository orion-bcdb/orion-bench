package config

import (
	"fmt"
	"os"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"gopkg.in/yaml.v3"
)

type OrionBenchConfig struct {
	lg     *logger.SugarLogger
	Cmd    *CommandLineArgs `yaml:"command-line"`
	Config types.YamlConfig `yaml:"config"`

	// Evaluated lazily
	material *material.BenchMaterial
	workload workload.Workload
}

func ReadConfig(cmd *CommandLineArgs) *OrionBenchConfig {
	binConfig, err := os.ReadFile(cmd.ConfigPath)
	utils.CheckDefault(err)

	c := &OrionBenchConfig{Cmd: cmd}
	utils.CheckDefault(yaml.Unmarshal(binConfig, &c.Config))

	loggerConf := &logger.Config{
		Level:         c.Config.LogLevel,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "orion-bench",
	}
	c.lg, err = logger.New(loggerConf)
	utils.CheckDefault(err)

	return c
}

func (c *OrionBenchConfig) Print() {
	s, err := yaml.Marshal(c)
	c.Check(err)
	fmt.Println(string(s))
}

func (c *OrionBenchConfig) Check(err error) {
	utils.Check(c.lg, err)
}

func (c *OrionBenchConfig) Material() *material.BenchMaterial {
	if c.material != nil {
		return c.material
	}

	c.material = material.New(&c.Config, c.lg)
	return c.material
}

func (c *OrionBenchConfig) Workload() workload.Workload {
	if c.workload != nil {
		return c.workload
	}

	c.workload = workload.New(c.Cmd.WorkerRank, &c.Config, c.Material(), c.lg)
	return c.workload
}
