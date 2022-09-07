package material

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/test/setup"
)

type BenchMaterial struct {
	lg     *logger.SugarLogger
	config *types.YamlConfig

	// Evaluated lazily
	crypto  map[string]*CryptoMaterial
	servers map[string]*ServerMaterial
}

func New(config *types.YamlConfig, lg *logger.SugarLogger) *BenchMaterial {
	return &BenchMaterial{
		lg:     lg,
		config: config,
	}
}

func userIndex(i int) string {
	return fmt.Sprintf(fmtUserIndex, i)
}

func (m *BenchMaterial) Check(err error) {
	utils.Check(m.lg, err)
}

func (m *BenchMaterial) UserIndex(i int) *CryptoMaterial {
	return m.User(userIndex(i))
}

func (m *BenchMaterial) getCrypto(name string, pathName string) *CryptoMaterial {
	if m.crypto == nil {
		m.crypto = make(map[string]*CryptoMaterial)
	}

	material, ok := m.crypto[pathName]
	if !ok {
		material = &CryptoMaterial{
			lg:   m.lg,
			name: name,
			path: filepath.Join(m.config.MaterialPath, pathName),
		}
		m.crypto[pathName] = material
	}
	return material
}

func (m *BenchMaterial) User(name string) *CryptoMaterial {
	return m.getCrypto(name, prefixUser+name)
}

func (m *BenchMaterial) Server(name string) *ServerMaterial {
	if m.servers == nil {
		m.servers = make(map[string]*ServerMaterial)
	}

	server, ok := m.servers[name]
	if !ok {
		pathName := prefixServer + name
		server = &ServerMaterial{
			lg:           m.lg,
			name:         name,
			materialPath: filepath.Join(m.config.MaterialPath, pathName),
			dataPath:     filepath.Join(m.config.DataPath, pathName),
			server:       m.config.Cluster[name],
			crypto:       m.getCrypto(name, pathName),
			rootCrypto:   m.User(Root),
		}
		m.servers[name] = server
	}
	return server
}

func (m *BenchMaterial) GenerateNUsers(n int) {
	users := make([]string, n)
	for i := 0; i < n; i++ {
		users[i] = userIndex(i)
	}
	m.Generate(users)
}

func (m *BenchMaterial) Generate(users []string) {
	m.Check(os.RemoveAll(m.config.MaterialPath))
	m.Check(os.MkdirAll(m.config.MaterialPath, perm))

	root := m.User(Root)
	root.generateRoot(userHost)

	for _, name := range append(users, Admin) {
		m.User(name).generate(root, userHost)
	}

	m.GenerateBootstrapFile()
	for name := range m.config.Cluster {
		m.Server(name).generate()
	}
}

func (m *BenchMaterial) List() []string {
	files, err := os.ReadDir(m.config.MaterialPath)
	m.Check(err)
	var material []string
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		ext := filepath.Ext(f.Name())
		if ext != ".key" {
			continue
		}
		material = append(material, strings.TrimSuffix(f.Name(), ext))
	}
	return material
}

func (m *BenchMaterial) ListUsers() []*CryptoMaterial {
	var users []*CryptoMaterial
	for _, material := range m.List() {
		if !strings.HasPrefix(material, prefixUser) {
			continue
		}
		userName := strings.TrimPrefix(material, prefixUser)
		users = append(users, m.User(userName))
	}
	return users
}

func (m *BenchMaterial) ServerTLS() sdkconfig.ServerTLSConfig {
	return sdkconfig.ServerTLSConfig{
		Enabled: false,
		//ClientAuthRequired: true,
		//CaConfig: config.CAConfiguration{
		//	RootCACertsPath:         []string{m.User(Root).CertPath()},
		//	IntermediateCACertsPath: nil,
		//},
	}
}

func (m *BenchMaterial) GenerateBootstrapFile() {
	sharedConfig := &config.SharedConfiguration{
		Nodes: nil,
		Consensus: &config.ConsensusConf{
			Algorithm: "raft",
			Members:   nil,
			Observers: nil,
			RaftConfig: &config.RaftConf{
				TickInterval:         "100ms",
				ElectionTicks:        50,
				HeartbeatTicks:       5,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 64 * 1024 * 1024,
			},
		},
		CAConfig: config.CAConfiguration{
			RootCACertsPath:         []string{m.User(Root).CertPath()},
			IntermediateCACertsPath: nil,
		},
		Admin: config.AdminConf{
			ID:              Admin,
			CertificatePath: m.User(Admin).CertPath(),
		},
	}

	for serverID, s := range m.config.Cluster {
		sharedConfig.Consensus.Members = append(
			sharedConfig.Consensus.Members,
			&config.PeerConf{
				NodeId:   serverID,
				RaftId:   s.RaftId,
				PeerHost: s.Address,
				PeerPort: s.PeerPort,
			},
		)
	}

	for serverID, s := range m.config.Cluster {
		sharedConfig.Nodes = append(sharedConfig.Nodes, &config.NodeConf{
			NodeID:          serverID,
			Host:            s.Address,
			Port:            s.NodePort,
			CertificatePath: m.Server(serverID).crypto.CertPath(),
		})
	}

	for serverID := range m.config.Cluster {
		m.Check(setup.WriteSharedConfig(sharedConfig, m.Server(serverID).BootstrapConfPath()))
	}
}
