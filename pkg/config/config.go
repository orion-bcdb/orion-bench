package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type OrionBenchConfig struct {
	lg         *logger.SugarLogger
	YamlConfig *types.YamlConfig `yaml:",inline"`

	// Evaluated lazily
	material *material.BenchMaterial
	db       bcdb.BCDB
	workload workload.Workload
	sessions map[string]bcdb.DBSession
}

func ReadConfig(path string) *OrionBenchConfig {
	binConfig, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read configuration file from: %s. With error: %s", path, err)
	}

	config := &types.YamlConfig{}
	err = yaml.Unmarshal(binConfig, config)
	if err != nil {
		log.Fatalf("Failed to parse configuration file from: %s. With error: %s", path, err)
	}

	loggerConf := &logger.Config{
		Level:         config.LogLevel,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "orion-bench",
	}
	lg, err := logger.New(loggerConf, zap.AddCallerSkip(0))
	if err != nil {
		log.Fatalf("Failed to create logger. With error: %s", err)
	}

	return &OrionBenchConfig{
		YamlConfig: config,
		lg:         lg,
	}
}

func (c *OrionBenchConfig) Check(err error) {
	if err != nil {
		utils.Check(c.lg, err)
	}
}

func (c *OrionBenchConfig) Log() *logger.SugarLogger {
	return c.lg
}

func (c *OrionBenchConfig) Config() *types.YamlConfig {
	return c.YamlConfig
}

func (c *OrionBenchConfig) Material() *material.BenchMaterial {
	if c.material != nil {
		return c.material
	}

	c.material = material.New(c.YamlConfig, c.lg)
	return c.material
}

func (c *OrionBenchConfig) Replicas() []*sdkconfig.Replica {
	var replicas []*sdkconfig.Replica
	for serverId, s := range c.YamlConfig.Cluster {
		replicas = append(replicas, &sdkconfig.Replica{
			ID:       serverId,
			Endpoint: "http://" + s.Address + ":" + strconv.Itoa(int(s.NodePort)),
		})
	}
	return replicas
}

func (c *OrionBenchConfig) DB() bcdb.BCDB {
	if c.db != nil {
		return c.db
	}

	db, err := bcdb.Create(&sdkconfig.ConnectionConfig{
		ReplicaSet: c.Replicas(),
		RootCAs:    []string{c.Material().User(material.Root).CertPath()},
		Logger:     c.lg,
		//TLSConfig:  c.Material().ServerTLS(),
	})
	c.Check(err)
	c.db = db
	return c.db
}

func (c *OrionBenchConfig) UserSession(user string) bcdb.DBSession {
	if c.sessions == nil {
		c.sessions = make(map[string]bcdb.DBSession)
	}

	session, ok := c.sessions[user]
	if ok {
		return session
	}

	userCrypto := c.Material().User(user)
	session, err := c.DB().Session(&sdkconfig.SessionConfig{
		UserConfig:   userCrypto.Config(),
		TxTimeout:    time.Duration(c.YamlConfig.Session.TxTimeout) * time.Second,
		QueryTimeout: time.Duration(c.YamlConfig.Session.QueryTimeout) * time.Second,
		//ClientTLS:    userCrypto.TLS(),
	})
	c.Check(err)

	c.sessions[user] = session
	return session
}

func (c *OrionBenchConfig) Workload() workload.Workload {
	if c.workload != nil {
		return c.workload
	}

	c.workload = workload.BuildWorkload(c)
	return c.workload
}
