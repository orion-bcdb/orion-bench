package types

type SessionConf struct {
	// The transaction timeout given to the database server in case of tx sync commit.
	TxTimeout int `yaml:"tx-timeout"`
	// The query timeout - SDK will wait for query result maximum `QueryTimeout` time.
	QueryTimeout int `yaml:"query-timeout"`
}

type Server struct {
	Address  string `yaml:"address"`
	RaftId   uint64 `yaml:"raft-id"`
	NodePort uint32 `yaml:"node-port"`
	PeerPort uint32 `yaml:"peer-port"`
}

type Cluster map[string]Server

type YamlConfig struct {
	LogLevel     string      `yaml:"log-level"`
	Session      SessionConf `yaml:"session"`
	MaterialPath string      `yaml:"material-path"`
	DataPath     string      `yaml:"data-path"`
	UserCount    int         `yaml:"user-count"`
	WorkloadName string      `yaml:"workload"`
	Cluster      Cluster     `yaml:"cluster"`
}
