package material

import (
	"fmt"
	"path/filepath"

	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/spf13/viper"
)

const (
	sharedConfSuffix = ".shared-config.yml"
	localConfSuffix  = ".local-config.yml"
)

type NodeMaterial struct {
	lg             *logger.SugarLogger
	materialPath   string
	dataPath       string
	Address        string
	RaftId         uint64
	NodePort       types.Port
	PeerPort       types.Port
	PrometheusPort types.Port
	Crypto         *CryptoMaterial
	material       *BenchMaterial

	// Evaluated lazily
	defaultConf *config.Configurations
}

func (s *NodeMaterial) Check(err error) {
	utils.Check(s.lg, err)
}

func (s *NodeMaterial) SharedConfPath() string {
	return s.materialPath + sharedConfSuffix
}

func (s *NodeMaterial) LocalConfPath() string {
	return s.materialPath + localConfSuffix
}

func (s *NodeMaterial) LedgerPath() string {
	return filepath.Join(s.dataPath, "ledger")
}

func (s *NodeMaterial) WalPath() string {
	return filepath.Join(s.dataPath, "etcdraft", "wal")
}

func (s *NodeMaterial) SnapPath() string {
	return filepath.Join(s.dataPath, "etcdraft", "snap")
}

func (s *NodeMaterial) PrometheusTargetAddress() string {
	return fmt.Sprintf("%s:%d", s.Address, s.PrometheusPort)
}

func (s *NodeMaterial) generate() {
	s.Crypto.generate(s.material.RootUser(), s.Address)
	s.GenerateSharedConfFile()
	s.GenerateServerConfigFile()
}

func (s *NodeMaterial) TLS() config.TLSConf {
	return config.TLSConf{
		Enabled:            false,
		ClientAuthRequired: false,
		//ServerCertificatePath: s.Crypto.CertPath(),
		//ServerKeyPath:         s.Crypto.KeyPath(),
		//ClientCertificatePath: s.Crypto.CertPath(),
		//ClientKeyPath:         s.Crypto.KeyPath(),
		//CaConfig: config.CAConfiguration{
		//	RootCACertsPath: []string{s.rootCrypto.CertPath()},
		//},
	}
}

func (s *NodeMaterial) readConfigFile(configFilePath string, conf interface{}) {
	v := viper.New()
	v.SetConfigFile(configFilePath)
	s.Check(v.ReadInConfig())
	s.Check(v.UnmarshalExact(conf))
}

func (s *NodeMaterial) DefaultConfiguration() *config.Configurations {
	if s.defaultConf == nil {
		s.defaultConf = &config.Configurations{}
		s.readConfigFile(s.material.config.Material.DefaultLocalConfPath, &s.defaultConf.LocalConfig)
		s.readConfigFile(s.material.config.Material.DefaultSharedConfPath, &s.defaultConf.SharedConfig)
	}
	return s.defaultConf
}

func (s *NodeMaterial) GenerateSharedConfFile() {
	sharedConfig := *s.DefaultConfiguration().SharedConfig

	sharedConfig.CAConfig = config.CAConfiguration{
		RootCACertsPath:         []string{s.material.RootUser().CertPath()},
		IntermediateCACertsPath: nil,
	}
	sharedConfig.Admin = config.AdminConf{
		ID:              s.material.AdminUser().name,
		CertificatePath: s.material.AdminUser().CertPath(),
	}

	allNodes := s.material.AllNodes()
	nNodes := len(allNodes)
	sharedConfig.Consensus.Members = make([]*config.PeerConf, nNodes)
	sharedConfig.Nodes = make([]*config.NodeConf, nNodes)
	for i, nodeData := range allNodes {
		sharedConfig.Consensus.Members[i] = &config.PeerConf{
			NodeId:   nodeData.Crypto.name,
			RaftId:   nodeData.RaftId,
			PeerHost: nodeData.Address,
			PeerPort: uint32(nodeData.PeerPort),
		}
		sharedConfig.Nodes[i] = &config.NodeConf{
			NodeID:          nodeData.Crypto.name,
			Host:            nodeData.Address,
			Port:            uint32(nodeData.NodePort),
			CertificatePath: nodeData.Crypto.CertPath(),
		}
	}

	s.Check(setup.WriteSharedConfig(&sharedConfig, s.SharedConfPath()))
}

func (s *NodeMaterial) GenerateServerConfigFile() {
	localConfig := *s.DefaultConfiguration().LocalConfig

	localConfig.Server.Identity = config.IdentityConf{
		ID:              s.Crypto.Name(),
		CertificatePath: s.Crypto.CertPath(),
		KeyPath:         s.Crypto.KeyPath(),
	}
	localConfig.Server.Network = config.NetworkConf{
		Address: s.Address,
		Port:    uint32(s.NodePort),
	}
	localConfig.Server.Database.LedgerDirectory = s.LedgerPath()
	localConfig.Replication.WALDir = s.WalPath()
	localConfig.Replication.SnapDir = s.SnapPath()
	localConfig.Replication.Network = config.NetworkConf{
		Address: s.Address,
		Port:    uint32(s.PeerPort),
	}
	localConfig.Bootstrap = config.BootstrapConf{
		Method: "genesis",
		File:   s.SharedConfPath(),
	}

	s.Check(setup.WriteLocalConfig(&localConfig, s.LocalConfPath()))
}

func (s *NodeMaterial) Run() {
	conf, err := config.Read(s.LocalConfPath())
	s.Check(err)
	srv, err := server.New(conf)
	s.Check(err)
	s.Check(srv.Start())
}
