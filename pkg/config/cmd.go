// Author: Liran Funaro <liran.funaro@ibm.com>

package config

import (
	"flag"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type OpFunction func(args *OrionBenchConfig)
type NamedOpFunction struct {
	Name        string
	Description string
	Selected    bool
	function    OpFunction
}

type CmdOperations struct {
	OpList []*NamedOpFunction
}

func NewCmd() *CmdOperations {
	return &CmdOperations{}
}

func (o *CmdOperations) Add(name string, description string, f OpFunction) *CmdOperations {
	o.OpList = append(o.OpList, &NamedOpFunction{
		Name:        name,
		Description: description,
		function:    f,
		Selected:    false,
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

type Rank struct {
	uint64
}

const (
	MainRank     = math.MaxUint64
	MainRankName = "main"
)

func (r *Rank) IsMainRank() bool {
	return r.uint64 == MainRank
}

func (r *Rank) Number() uint64 {
	return r.uint64
}

func (r *Rank) String() string {
	if r.IsMainRank() {
		return MainRankName
	}
	return strconv.FormatInt(int64(r.uint64), 10)
}

func (r *Rank) Set(value string) error {
	value = strings.ToLower(value)
	if value == MainRankName || value == "" {
		r.uint64 = MainRank
	} else {
		number, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "'%s' is not a rank", value)
		}
		r.uint64 = number
	}

	return nil
}

func (r *Rank) MarshalYAML() (interface{}, error) {
	if r.IsMainRank() {
		return MainRankName, nil
	}
	return r.uint64, nil
}

type CommandLineArgs struct {
	Cwd        string         `yaml:"cwd"`
	ConfigPath string         `yaml:"config-path"`
	Op         *CmdOperations `yaml:"op,flow"`
	Rank       *Rank          `yaml:"rank"`
}

func ParseCommandLine(ops *CmdOperations) *CommandLineArgs {
	args := &CommandLineArgs{Op: ops, Rank: &Rank{MainRank}}
	flag.StringVar(&args.Cwd, "cwd", "",
		"benchmark configuration working directory")
	flag.StringVar(&args.ConfigPath, "config", "",
		"benchmark configuration YAML file path")
	for _, op := range ops.OpList {
		flag.BoolVar(&op.Selected, op.Name, false,
			fmt.Sprintf("[action]: %s", op.Description))
	}
	flag.Var(args.Rank, "rank",
		"worker/node rank (starting from 0)")
	flag.Parse()

	if args.ConfigPath == "" {
		log.Fatalf("Empty config path")
	}
	return args
}
