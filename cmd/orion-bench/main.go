// Author: Liran Funaro <liran.funaro@ibm.com>

package main

import (
	"log"
	"os"
	"sync"

	"orion-bench/pkg/config"
)

func main() {
	ops := config.NewCmd().Add(
		"clear", "clear all the material and data", func(c *config.OrionBenchConfig) {
			c.Check(os.RemoveAll(c.Config.Material.MaterialPath))
			c.Check(os.RemoveAll(c.Config.Material.DataPath))
		}).Add(
		"material", "generate all crypto material and configurations", func(c *config.OrionBenchConfig) {
			c.Material().Generate()
		}).Add(
		"list", "list all the available material", func(c *config.OrionBenchConfig) {
			log.Println(c.Material().List())
		}).Add(
		"node", "runs an orion node", func(c *config.OrionBenchConfig) {
			c.Node().Run()
			var wg sync.WaitGroup
			wg.Add(1)
			wg.Wait()
		}).Add(
		"init", "initialize the data for the benchmark", func(c *config.OrionBenchConfig) {
			c.Workload().Init()
		}).Add(
		"workload", "runs a workload generator (client)", func(c *config.OrionBenchConfig) {
			c.Workload().Run()
		}).Add(
		"prometheus", "runs a prometheus server to collect the data", func(c *config.OrionBenchConfig) {
			c.Material().Prometheus().Run()
		})
	cmd := config.ParseCommandLine(ops)
	conf := config.ReadConfig(cmd)
	conf.Print()
	ops.ApplyAll(conf)
}
