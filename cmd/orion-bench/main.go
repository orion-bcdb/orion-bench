// Author: Liran Funaro <liran.funaro@ibm.com>

package main

import (
	"log"
	"os"

	"orion-bench/pkg/config"
)

func main() {
	ops := config.NewCmd().Add(
		"clear", "clear all the material and data", func(c *config.OrionBenchConfig) {
			c.Check(os.RemoveAll(c.Config.Path.Material))
			c.Check(os.RemoveAll(c.Config.Path.Data))
			c.Check(os.RemoveAll(c.Config.Path.Metrics))
		}).Add(
		"material", "generate all crypto material and configurations", func(c *config.OrionBenchConfig) {
			c.Material().Generate()
		}).Add(
		"list", "list all the available material", func(c *config.OrionBenchConfig) {
			log.Println(c.Material().List())
		}).Add(
		"node", "runs an orion node", func(c *config.OrionBenchConfig) {
			c.Node().RunAndWait()
		}).Add(
		"init", "initialize the data for the benchmark", func(c *config.OrionBenchConfig) {
			c.Workload().Init()
		}).Add(
		"warmup", "runs a workload generator (client) for warmup", func(c *config.OrionBenchConfig) {
			c.Workload().RunWarmup()
		}).Add(
		"benchmark", "runs a workload generator (client) for benchmark", func(c *config.OrionBenchConfig) {
			c.Workload().RunBenchmark()
		}).Add(
		"prometheus", "runs a prometheus server to collect the data", func(c *config.OrionBenchConfig) {
			c.Material().Prometheus().Run()
		})
	cmd := config.ParseCommandLine(ops)
	conf := config.ReadConfig(cmd)
	conf.Print()
	ops.ApplyAll(conf)
}
