package config

import (
	"log"
	"os"
	"time"

	"orion-bench/pkg/material"
	"orion-bench/pkg/types"
	"orion-bench/pkg/utils"
	"orion-bench/pkg/workload"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	sdkconfig "github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"gopkg.in/yaml.v3"
)

type OrionBenchConfig struct {
	types.YamlConfig `yaml:",inline"`

	// Evaluated lazily
	lg       *logger.SugarLogger       `yaml:"-"`
	crypto   *material.BenchMaterial   `yaml:"-"`
	db       bcdb.BCDB                 `yaml:"-"`
	workload workload.Workload         `yaml:"-"`
	sessions map[string]bcdb.DBSession `yaml:"-"`
}

func ReadConfig(path string) *OrionBenchConfig {
	binConfig, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read configuration file from: %s. With error: %s", path, err)
	}

	config := &OrionBenchConfig{}
	err = yaml.Unmarshal(binConfig, &config)
	if err != nil {
		log.Fatalf("Failed to parse configuration file from: %s. With error: %s", path, err)
	}

	return config
}

func (c *OrionBenchConfig) Log() *logger.SugarLogger {
	if c.lg == nil {
		loggerConf := &logger.Config{
			Level:         c.LogLevel,
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "orion-bench",
		}
		lg, err := logger.New(loggerConf)
		if err != nil {
			log.Fatalf("Failed to create logger. With error: %s", err)
		}
		c.lg = lg
	}
	return c.lg
}

func (c *OrionBenchConfig) Check(err error) {
	utils.Check(c.lg, err)
}

func (c *OrionBenchConfig) Material() *material.BenchMaterial {
	if c.crypto == nil {
		c.crypto = &material.BenchMaterial{
			MaterialPath: c.MaterialPath,
			DataPath:     c.DataPath,
			Cluster:      c.Cluster,
		}
	}
	return c.crypto
}

func (c *OrionBenchConfig) DB() bcdb.BCDB {
	if c.db == nil {
		c.Log().Infof("Servers: %v", c.Cluster)

		var replicas []*sdkconfig.Replica
		for serverId, s := range c.Cluster {
			replicas = append(replicas, &sdkconfig.Replica{
				ID:       serverId,
				Endpoint: s.Address,
			})
		}
		db, err := bcdb.Create(&sdkconfig.ConnectionConfig{
			ReplicaSet: replicas,
			RootCAs:    []string{c.Material().User(material.Root).CertPath()},
			Logger:     c.Log(),
		})
		c.Check(err)
		c.db = db
	}
	return c.db
}

func (c *OrionBenchConfig) Workload() workload.Workload {
	if c.workload == nil {
		c.workload = workload.BuildWorkload(c.WorkloadName, c)
	}
	return c.workload
}

func (c *OrionBenchConfig) UserSession(user string) bcdb.DBSession {
	session, ok := c.sessions[user]
	if ok {
		return session
	}

	session, err := c.DB().Session(&sdkconfig.SessionConfig{
		UserConfig:   c.Material().User(user).Config(),
		TxTimeout:    time.Duration(c.Session.TxTimeout) * time.Second,
		QueryTimeout: time.Duration(c.Session.QueryTimeout) * time.Second,
	})
	c.Check(err)

	c.sessions[user] = session
	return session
}
