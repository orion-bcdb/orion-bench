// Author: Liran Funaro <liran.funaro@ibm.com>

package types

import (
	"time"
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
	NodeBasePort       Port     `yaml:"node-base-port"`
	PeerBasePort       Port     `yaml:"peer-base-port"`
	PrometheusBasePort Port     `yaml:"prometheus-base-port"`
	Nodes              []string `yaml:"nodes"`
}

type WorkloadConf struct {
	Name               string            `yaml:"name"`
	UserCount          uint64            `yaml:"user-count"`
	Session            SessionConf       `yaml:"session"`
	Duration           time.Duration     `yaml:"duration"`
	LogReportInterval  time.Duration     `yaml:"log-report-interval"`
	PrometheusBasePort Port              `yaml:"prometheus-base-port"`
	Workers            []string          `yaml:"workers"`
	Parameters         map[string]string `yaml:"parameters"`
}

type SessionConf struct {
	TxTimeout    int `yaml:"tx-timeout"`
	QueryTimeout int `yaml:"query-timeout"`
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
