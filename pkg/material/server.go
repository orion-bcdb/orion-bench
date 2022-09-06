package material

import (
	"crypto/tls"
	"path/filepath"
	"time"

	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/test/setup"
)

const (
	bootstrapConfExt = ".bootstrap-config.yml"
	localConfExt     = ".local-conf.yml"
)

type ServerMaterial struct {
	lg           *logger.SugarLogger
	materialPath string
	dataPath     string
	name         string
	server       types.Server
	crypto       *CryptoMaterial
}

func (s *ServerMaterial) Check(err error) {
	utils.Check(s.lg, err)
}

func (s *ServerMaterial) BootstrapConfPath() string {
	return s.materialPath + bootstrapConfExt
}

func (s *ServerMaterial) LocalConfPath() string {
	return s.materialPath + localConfExt
}

func (s *ServerMaterial) LedgerPath() string {
	return filepath.Join(s.dataPath, "ledger")
}

func (s *ServerMaterial) WalPath() string {
	return filepath.Join(s.dataPath, "etcdraft", "wal")
}

func (s *ServerMaterial) SnapPath() string {
	return filepath.Join(s.dataPath, "etcdraft", "snap")
}

func (s *ServerMaterial) generate(rootCA tls.Certificate) {
	s.crypto.generate(rootCA, s.server.Address)
	s.GenerateServerConfigFile()
}

func (s *ServerMaterial) GenerateServerConfigFile() {
	localConfig := &config.LocalConfiguration{
		Server: config.ServerConf{
			Identity: config.IdentityConf{
				ID:              s.name,
				CertificatePath: s.crypto.CertPath(),
				KeyPath:         s.crypto.KeyPath(),
			},
			Network: config.NetworkConf{
				Address: s.server.Address,
				Port:    s.server.NodePort,
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: s.LedgerPath(),
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               1000,
				ReorderedTransactionBatch: 100,
				Block:                     100,
			},
			LogLevel: "info",
		},
		BlockCreation: config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 10,
			BlockTimeout:                50 * time.Millisecond,
		},
		Replication: config.ReplicationConf{
			WALDir:  s.WalPath(),
			SnapDir: s.SnapPath(),
			Network: config.NetworkConf{
				Address: s.server.Address,
				Port:    s.server.NodePort,
			},
			TLS: config.TLSConf{
				Enabled: false,
			},
		},
		Bootstrap: config.BootstrapConf{
			Method: "genesis",
			File:   s.BootstrapConfPath(),
		},
	}

	s.Check(setup.WriteLocalConfig(localConfig, s.LocalConfPath()))
}
