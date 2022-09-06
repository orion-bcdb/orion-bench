package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"orion-bench/pkg/config"
)

type OpFunction func(args *CommandLineArgs)

type CommandLineArgs struct {
	ConfigPath string                   `json:"ConfigPath"`
	Op         string                   `json:"Op"`
	Worker     int                      `json:"Worker"`
	Config     *config.OrionBenchConfig `json:"Config"`
	OpFunc     OpFunction               `json:"-"`
}

func ParseCommandLine(ops map[string]OpFunction) *CommandLineArgs {
	var opNames []string
	for k := range ops {
		opNames = append(opNames, k)
	}

	args := &CommandLineArgs{}

	flag.StringVar(
		&args.ConfigPath,
		"config",
		"",
		"Benchmark configuration YAML file path.",
	)
	flag.StringVar(
		&args.Op,
		"op",
		"",
		fmt.Sprintf("Benchmark operation %v.", opNames),
	)
	flag.IntVar(
		&args.Worker,
		"worker",
		0,
		"Worker rank.",
	)
	flag.Parse()

	args.Config = config.ReadConfig(args.ConfigPath)

	opFunc, ok := ops[args.Op]
	if !ok {
		log.Fatalf("Invalid operation: %s", args.Op)
	}
	args.OpFunc = opFunc

	if args.Worker < 0 {
		log.Fatalf("Invalid worker rang: %d", args.Worker)
	}

	return args
}

func (a *CommandLineArgs) Print() {
	s, err := json.MarshalIndent(a, "", "  ")
	a.Config.Check(err)
	fmt.Println(string(s))
}
