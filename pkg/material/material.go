package material

import (
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/test/setup"
)

type BenchMaterial struct {
	lg           *logger.SugarLogger
	MaterialPath string
	DataPath     string
	Cluster      types.Cluster

	// Evaluated lazily
	users   map[string]*CryptoMaterial
	servers map[string]*ServerMaterial
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

func (m *BenchMaterial) getCrypto(name string) *CryptoMaterial {
	if m.users == nil {
		m.users = make(map[string]*CryptoMaterial)
	}

	material, ok := m.users[name]
	if !ok {
		material = &CryptoMaterial{
			lg:   m.lg,
			name: name,
			path: filepath.Join(m.MaterialPath, name),
		}
		m.users[name] = material
	}
	return material
}

func (m *BenchMaterial) User(name string) *CryptoMaterial {
	return m.getCrypto(fmt.Sprintf(fmtUser, name))
}

func (m *BenchMaterial) Server(name string) *ServerMaterial {
	if m.servers == nil {
		m.servers = make(map[string]*ServerMaterial)
	}

	server, ok := m.servers[name]
	if !ok {
		serverPrefix := fmt.Sprintf(fmtServer, name)
		server = &ServerMaterial{
			lg:           m.lg,
			name:         name,
			materialPath: filepath.Join(m.MaterialPath, serverPrefix),
			dataPath:     filepath.Join(m.DataPath, serverPrefix),
			server:       m.Cluster[name],
			crypto:       m.getCrypto(serverPrefix),
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
	m.Check(os.RemoveAll(m.MaterialPath))
	m.Check(os.MkdirAll(m.MaterialPath, perm))

	rootUser := m.User(Root)
	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA(rootUser.subject(), userHost)
	m.Check(err)
	rootUser.write(rootCAPemCert, caPrivKey)

	rootCAkeyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	m.Check(err)
	for _, name := range append(users, Admin) {
		m.User(name).generate(rootCAkeyPair, userHost)
	}

	m.GenerateBootstrapFile()
	for name := range m.Cluster {
		m.Server(name).generate(rootCAkeyPair)
	}
}

func (m *BenchMaterial) List() []string {
	files, err := os.ReadDir(m.MaterialPath)
	m.Check(err)
	var users []string
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		ext := filepath.Ext(f.Name())
		if ext != ".key" {
			continue
		}
		users = append(users, strings.TrimSuffix(f.Name(), ext))
	}
	return users
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

	for serverID, s := range m.Cluster {
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

	for serverID, s := range m.Cluster {
		sharedConfig.Nodes = append(sharedConfig.Nodes, &config.NodeConf{
			NodeID:          serverID,
			Host:            s.Address,
			Port:            s.NodePort,
			CertificatePath: m.Server(serverID).crypto.CertPath(),
		})
	}

	for serverID := range m.Cluster {
		m.Check(setup.WriteSharedConfig(sharedConfig, m.Server(serverID).BootstrapConfPath()))
	}
}
