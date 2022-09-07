package cmd

import (
	"flag"
	"fmt"
	"log"

	"orion-bench/pkg/config"

	"gopkg.in/yaml.v3"
)

type OpFunction func(args *CommandLineArgs)
type opFlag []string

func (o *opFlag) String() string {
	return fmt.Sprintf("%v", ([]string)(*o))
}

func (o *opFlag) Set(value string) error {
	*o = append(*o, value)
	return nil
}

type CommandLineArgs struct {
	ConfigPath string                   `yaml:"config-path"`
	Op         opFlag                   `yaml:"op"`
	Server     string                   `yaml:"server"`
	Worker     int                      `yaml:"worker"`
	Config     *config.OrionBenchConfig `yaml:"config"`
	OpFunc     []OpFunction             `yaml:"-"`
}

func ParseCommandLine(ops map[string]OpFunction) *CommandLineArgs {
	var opNames []string
	for k := range ops {
		opNames = append(opNames, k)
	}

	args := &CommandLineArgs{}
	flag.StringVar(&args.ConfigPath, "config", "",
		"Benchmark configuration YAML file path.")
	flag.Var(&args.Op, "op",
		fmt.Sprintf("Benchmark operation %v.", opNames))
	flag.StringVar(&args.Server, "server", "localhost",
		"Current server.")
	flag.IntVar(&args.Worker, "worker", 0,
		"Worker rank.")
	flag.Parse()

	args.Config = config.ReadConfig(args.ConfigPath)

	for _, o := range args.Op {
		opFunc, ok := ops[o]
		if !ok {
			log.Fatalf("Invalid operation: %s", o)
		}
		args.OpFunc = append(args.OpFunc, opFunc)
	}

	if args.Worker < 0 {
		log.Fatalf("Invalid worker rang: %d", args.Worker)
	}

	_, ok := args.Config.YamlConfig.Cluster[args.Server]
	if !ok {
		log.Fatalf("Invalid server name: %s", args.Server)
	}

	return args
}

func (a *CommandLineArgs) Print() {
	s, err := yaml.Marshal(a)
	a.Config.Check(err)
	fmt.Println(string(s))
}
