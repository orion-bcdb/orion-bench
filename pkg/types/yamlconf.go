// Author: Liran Funaro <liran.funaro@ibm.com>

package types

import (
	"time"

	"github.com/creasty/defaults"
)

type Port uint32

type PathConf struct {
	Material              string `yaml:"material"`
	Data                  string `yaml:"data"`
	Metrics               string `yaml:"metrics"`
	DefaultLocalConf      string `yaml:"default-local-conf"`
	DefaultSharedConf     string `yaml:"default-shared-conf"`
	DefaultPrometheusConf string `yaml:"default-prometheus-conf"`
}

type ClusterConf struct {
	NodeBasePort               Port          `yaml:"node-base-port"`
	PeerBasePort               Port          `yaml:"peer-base-port"`
	PrometheusBasePort         Port          `yaml:"prometheus-base-port"`
	DataSizeCollectionInterval time.Duration `yaml:"data-size-collection-interval"`
	Nodes                      []string      `yaml:"nodes"`
}

type WorkloadConf struct {
	Name               string                 `yaml:"name"`
	UserCount          uint64                 `yaml:"user-count"`
	Distributions      []WorkloadDistribution `yaml:"distributions"`
	Session            SessionConf            `yaml:"session"`
	Duration           time.Duration          `yaml:"duration"`
	WarmupDuration     time.Duration          `yaml:"warmup-duration"`
	PrometheusBasePort Port                   `yaml:"prometheus-base-port"`
	Workers            []string               `yaml:"workers"`
	Parameters         map[string]string      `yaml:"parameters"`
}

type SessionConf struct {
	TxTimeout    time.Duration `yaml:"tx-timeout"`
	QueryTimeout time.Duration `yaml:"query-timeout"`
	Backoff      BackoffConf   `yaml:"backoff"`
}

type WorkloadDistribution struct {
	Percent   uint32 `yaml:"percent"`
	Operation string `yaml:"operation"`
}

type BackoffConf struct {
	InitialInterval     time.Duration `default:"10ms" yaml:"initial-interval"`
	RandomizationFactor float64       `default:"0.5" yaml:"randomization-factor"`
	Multiplier          float64       `default:"1.5" yaml:"multiplier"`
	MaxInterval         time.Duration `default:"1s" yaml:"max-interval"`
	MaxElapsedTime      time.Duration `default:"60s" yaml:"max-elapsed-time"`
}

type PrometheusConf struct {
	ListenAddress string `yaml:"listen-address"`
}

type BenchmarkConf struct {
	LogLevel   string         `yaml:"log-level"`
	Path       PathConf       `yaml:"path"`
	Cluster    ClusterConf    `yaml:"cluster"`
	Workload   WorkloadConf   `yaml:"workload"`
	Prometheus PrometheusConf `yaml:"prometheus"`
}

func (s *BenchmarkConf) UnmarshalYAML(unmarshal func(interface{}) error) error {
	err := defaults.Set(s)
	if err != nil {
		return err
	}

	type plain BenchmarkConf
	if err = unmarshal((*plain)(s)); err != nil {
		return err
	}

	return nil
}
