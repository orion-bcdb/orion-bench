// Author: Liran Funaro <liran.funaro@ibm.com>

package material

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"

	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type BenchMaterial struct {
	lg         *logger.SugarLogger
	config     *types.BenchmarkConf
	crypto     sync.Map
	servers    sync.Map
	prometheus *PrometheusMaterial
}

func New(config *types.BenchmarkConf, lg *logger.SugarLogger) *BenchMaterial {
	return &BenchMaterial{
		lg:     lg,
		config: config,
	}
}

func userIndex(i uint64) string {
	return fmt.Sprintf(fmtUserIndex, i)
}

func nodeIndex(i uint64) string {
	return fmt.Sprintf(fmtNodeIndex, i)
}

func (m *BenchMaterial) Check(err error) {
	utils.Check(m.lg, err)
}

func (m *BenchMaterial) getCrypto(name string, pathName string) *CryptoMaterial {
	material, ok := m.crypto.Load(pathName)
	if ok {
		return material.(*CryptoMaterial)
	}

	material = &CryptoMaterial{
		lg:   m.lg,
		name: name,
		path: filepath.Join(m.config.Path.Material, pathName),
	}
	actualMaterial, _ := m.crypto.LoadOrStore(pathName, material)
	return actualMaterial.(*CryptoMaterial)
}

func (m *BenchMaterial) getUserCrypto(name string) *CryptoMaterial {
	return m.getCrypto(name, prefixUser+name)
}

func (m *BenchMaterial) RootUser() *CryptoMaterial {
	return m.getUserCrypto(Root)
}

func (m *BenchMaterial) AdminUser() *CryptoMaterial {
	return m.getUserCrypto(Admin)
}

func (m *BenchMaterial) User(i uint64) *CryptoMaterial {
	return m.getUserCrypto(userIndex(i))
}

func (m *BenchMaterial) Node(i uint64) *NodeMaterial {
	name := nodeIndex(i)
	server, ok := m.servers.Load(name)
	if ok {
		return server.(*NodeMaterial)
	}
	pathName := prefixNode + name
	server = &NodeMaterial{
		lg:             m.lg,
		rank:           i,
		materialPath:   filepath.Join(m.config.Path.Material, pathName),
		dataPath:       filepath.Join(m.config.Path.Data, pathName),
		Address:        m.config.Cluster.Nodes[i],
		RaftId:         i + 1,
		NodePort:       m.config.Cluster.NodeBasePort + types.Port(i),
		PeerPort:       m.config.Cluster.PeerBasePort + types.Port(i),
		PrometheusPort: m.config.Cluster.PrometheusBasePort + types.Port(i),
		Crypto:         m.getCrypto(name, pathName),
		material:       m,
	}
	actualServer, _ := m.crypto.LoadOrStore(name, server)
	return actualServer.(*NodeMaterial)
}

func (m *BenchMaterial) Worker(i uint64) *WorkerMaterial {
	return &WorkerMaterial{
		lg:             m.lg,
		Rank:           i,
		Address:        m.config.Workload.Workers[i],
		PrometheusPort: m.config.Workload.PrometheusBasePort + types.Port(i),
	}
}

func (m *BenchMaterial) Prometheus() *PrometheusMaterial {
	return &PrometheusMaterial{
		lg:              m.lg,
		material:        m,
		path:            filepath.Join(m.config.Path.Material, "prometheus.yaml"),
		defaultConfPath: m.config.Path.DefaultPrometheusConf,
	}
}

func (m *BenchMaterial) Generate() {
	m.Check(os.RemoveAll(m.config.Path.Material))
	m.Check(os.MkdirAll(m.config.Path.Material, perm))

	root := m.RootUser()
	root.generateRoot()
	m.AdminUser().generate(root, userHost)

	var wg sync.WaitGroup
	for _, user := range m.AllUsers() {
		wg.Add(1)
		go func(user *CryptoMaterial) {
			user.generate(root, userHost)
			wg.Done()
		}(user)
	}

	for _, node := range m.AllNodes() {
		wg.Add(1)
		go func(node *NodeMaterial) {
			node.generate()
			wg.Done()
		}(node)
	}

	wg.Wait()

	m.Prometheus().Generate()
}

func (m *BenchMaterial) List() []string {
	files, err := os.ReadDir(m.config.Path.Material)
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

func (m *BenchMaterial) AllUsers() []*CryptoMaterial {
	var users []*CryptoMaterial
	for i := uint64(0); i < m.config.Workload.UserCount; i++ {
		users = append(users, m.User(i))
	}
	return users
}

func (m *BenchMaterial) AllNodes() []*NodeMaterial {
	var servers []*NodeMaterial
	for i := range m.config.Cluster.Nodes {
		servers = append(servers, m.Node(uint64(i)))
	}
	return servers
}

func (m *BenchMaterial) AllWorkers() []*WorkerMaterial {
	var workers []*WorkerMaterial
	for i := range m.config.Workload.Workers {
		workers = append(workers, m.Worker(uint64(i)))
	}
	return workers
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
