package main

import (
	"log"

	"orion-bench/pkg/cmd"
)

var ops = map[string]cmd.OpFunction{
	"material": func(args *cmd.CommandLineArgs) {
		args.Config.Material().GenerateNUsers(args.Config.UserCount)
		log.Println(args.Config.Material().List())
	},
	"list": func(args *cmd.CommandLineArgs) {
		log.Println(args.Config.Material().List())
	},
	"init": func(args *cmd.CommandLineArgs) {
		args.Config.Workload().Init()
	},
	"run": func(args *cmd.CommandLineArgs) {
		args.Config.Workload().Run(args.Worker)
	},
}

func main() {
	args := cmd.ParseCommandLine(ops)
	args.Print()
	args.OpFunc(args)
}
