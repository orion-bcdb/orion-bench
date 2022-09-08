package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

type OpNameList []string

func (o *OpNameList) String() string {
	return strings.Join(*o, `, `)
}

func (o *OpNameList) Set(value string) error {
	*o = append(*o, value)
	return nil
}

func (o *OpNameList) Exist(value string) bool {
	for _, v := range *o {
		if v == value {
			return true
		}
	}
	return false
}

type OpFunction func(args *OrionBenchConfig)
type OrderedOpFunction struct {
	name     string
	function OpFunction
}
type OpMap []OrderedOpFunction

func (m *OpMap) Add(name string, f OpFunction) *OpMap {
	*m = append(*m, OrderedOpFunction{
		name:     name,
		function: f,
	})
	return m
}

func (m *OpMap) Names() *OpNameList {
	opFunctions := make(OpNameList, len(*m))
	for i, f := range *m {
		opFunctions[i] = f.name
	}
	return &opFunctions
}

func (m *OpMap) Exist(value string) bool {
	for _, v := range *m {
		if v.name == value {
			return true
		}
	}
	return false
}

func (m *OpMap) Validate(names OpNameList) {
	for _, v := range names {
		if !m.Exist(v) {
			log.Fatalf("Invalid op: '%s'. Options: %s.", v, m.Names())
		}
	}
}

func (m *OpMap) Subset(names OpNameList) OpMap {
	opFunctions := OpMap{}
	for _, f := range *m {
		if names.Exist(f.name) {
			opFunctions.Add(f.name, f.function)
		}
	}
	return opFunctions
}

func (o *OpNameList) ApplyAll(c *OrionBenchConfig) {
	for _, f := range ops.Subset(*o) {
		f.function(c)
	}
}

var ops = (&OpMap{}).Add(
	"clear", func(c *OrionBenchConfig) {
		c.Check(os.RemoveAll(c.Config.MaterialPath))
		c.Check(os.RemoveAll(c.Config.DataPath))
	}).Add(
	"material", func(c *OrionBenchConfig) {
		c.Material().GenerateNUsers(c.Config.Workload.UserCount)
		log.Println(c.Material().List())
	}).Add(
	"list", func(c *OrionBenchConfig) {
		log.Println(c.Material().List())
	}).Add(
	"users", func(c *OrionBenchConfig) {
		log.Println(c.Material().ListUserNames())
	}).Add(
	"init", func(c *OrionBenchConfig) {
		c.Workload().Init()
	}).Add(
	"run", func(c *OrionBenchConfig) {
		c.Workload().Run()
	})

type CommandLineArgs struct {
	ConfigPath string     `yaml:"config-path"`
	Op         OpNameList `yaml:"op"`
	WorkerRank int        `yaml:"worker"`
}

func ParseCommandLine() *CommandLineArgs {
	args := &CommandLineArgs{}
	flag.StringVar(&args.ConfigPath, "config", "",
		"Benchmark configuration YAML file path.")
	flag.Var(&args.Op, "op",
		fmt.Sprintf("Benchmark operation: %s.", ops.Names()))
	flag.IntVar(&args.WorkerRank, "worker", 0,
		"Worker rank (starting from 0).")
	flag.Parse()

	if args.ConfigPath == "" {
		log.Fatalf("Empty config path")
	}

	if args.WorkerRank < 0 {
		log.Fatalf("Invalid worker rank: %d", args.WorkerRank)
	}

	ops.Validate(args.Op)
	return args
}
