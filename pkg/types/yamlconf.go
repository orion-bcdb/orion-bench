package types

import (
	"time"
)

type Port uint32

type MaterialConf struct {
	MaterialPath              string `yaml:"material-path"`
	DataPath                  string `yaml:"data-path"`
	DefaultLocalConfPath      string `yaml:"default-local-conf-path"`
	DefaultSharedConfPath     string `yaml:"default-shared-conf-path"`
	DefaultPrometheusConfPath string `yaml:"default-prometheus-conf-path"`
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

type Server struct {
	Address  string `yaml:"address"`
	RaftId   uint64 `yaml:"raft-id"`
	NodePort uint32 `yaml:"node-port"`
	PeerPort uint32 `yaml:"peer-port"`
}

type BenchmarkConf struct {
	LogLevel string            `yaml:"log-level"`
	Material MaterialConf      `yaml:"material"`
	Machines map[string]string `yaml:"machines"`
	Cluster  ClusterConf       `yaml:"cluster"`
	Workload WorkloadConf      `yaml:"workload"`
}
