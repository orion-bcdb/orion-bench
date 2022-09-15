package config

import (
	"flag"
	"fmt"
	"log"
)

type OpFunction func(args *OrionBenchConfig)
type NamedOpFunction struct {
	Name     string
	Selected bool
	function OpFunction
}
type CmdOperations struct {
	OpList []*NamedOpFunction
}

func NewCmd() *CmdOperations {
	return &CmdOperations{}
}

func (o *CmdOperations) Add(name string, f OpFunction) *CmdOperations {
	o.OpList = append(o.OpList, &NamedOpFunction{
		Name:     name,
		function: f,
		Selected: false,
	})
	return o
}

func (o *CmdOperations) ApplyAll(conf *OrionBenchConfig) {
	for _, op := range o.OpList {
		if op.Selected {
			conf.lg.Infof("Running operation: %s", op.Name)
			op.function(conf)
		}
	}
}

func (o *CmdOperations) MarshalYAML() (interface{}, error) {
	var ret []string
	for _, f := range o.OpList {
		if f.Selected {
			ret = append(ret, f.Name)
		}
	}
	return ret, nil
}

type CommandLineArgs struct {
	ConfigPath string         `yaml:"config-path"`
	Op         *CmdOperations `yaml:"op,flow"`
	Rank       uint64         `yaml:"rank"`
}

func ParseCommandLine(ops *CmdOperations) *CommandLineArgs {
	args := &CommandLineArgs{Op: ops}
	flag.StringVar(&args.ConfigPath, "config", "",
		"Benchmark configuration YAML file path.")
	for _, op := range ops.OpList {
		flag.BoolVar(&op.Selected, op.Name, false,
			fmt.Sprintf("Benchmark operation: %s.", op.Name))
	}
	flag.Uint64Var(&args.Rank, "rank", 0,
		"Worker/node rank (starting from 0).")
	flag.Parse()

	if args.ConfigPath == "" {
		log.Fatalf("Empty config path")
	}

	return args
}
