package main

import (
	"orion-bench/pkg/config"
)

func main() {
	conf := config.ReadConfig(config.ParseCommandLine())
	conf.Print()
	conf.Cmd.Op.ApplyAll(conf)
}
