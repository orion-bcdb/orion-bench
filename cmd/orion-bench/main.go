package main

import (
	"log"
	"os"
	"sync"

	"orion-bench/pkg/config"
)

func main() {
	ops := config.NewCmd().Add(
		"clear", func(c *config.OrionBenchConfig) {
			c.Check(os.RemoveAll(c.Config.Material.MaterialPath))
			c.Check(os.RemoveAll(c.Config.Material.DataPath))
		}).Add(
		"material", func(c *config.OrionBenchConfig) {
			c.Material().Generate()
		}).Add(
		"list", func(c *config.OrionBenchConfig) {
			log.Println(c.Material().List())
		}).Add(
		"init", func(c *config.OrionBenchConfig) {
			c.Workload().Init()
		}).Add(
		"workload", func(c *config.OrionBenchConfig) {
			c.Workload().Run()
		}).Add(
		"node", func(c *config.OrionBenchConfig) {
			c.Node().Run()
			var wg sync.WaitGroup
			wg.Add(1)
			wg.Wait()
		})
	cmd := config.ParseCommandLine(ops)
	conf := config.ReadConfig(cmd)
	conf.Print()
	ops.ApplyAll(conf)
}
