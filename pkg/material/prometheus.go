// Author: Liran Funaro <liran.funaro@ibm.com>

package material

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type PrometheusMaterial struct {
	lg              *logger.SugarLogger
	path            string
	defaultConfPath string
	material        *BenchMaterial

	// Evaluated lazily
	conf *utils.Map
}

func (p *PrometheusMaterial) Check(err error) {
	utils.Check(p.lg, err)
}

func (p *PrometheusMaterial) readConfigFile(configFilePath string) *utils.Map {
	b, err := ioutil.ReadFile(configFilePath)
	p.Check(err)
	conf := utils.Unmarshal(b).Map()
	p.Check(conf.GetError())
	return conf
}

func (p *PrometheusMaterial) writeConfigFile(configFilePath string) {
	b, err := p.Conf().Marshal()
	p.Check(err)
	p.Check(ioutil.WriteFile(configFilePath, b, perm))
}

func (p *PrometheusMaterial) Conf() *utils.Map {
	if p.conf == nil {
		p.conf = p.readConfigFile(p.defaultConfPath)
	}
	return p.conf
}

func (p *PrometheusMaterial) MainScrapeConfig() *utils.Map {
	scrapeConfigs := p.Conf().SetDefaultList("scrape_configs")
	for i := 0; i < scrapeConfigs.Len(); i++ {
		if curConf := scrapeConfigs.Get(i).Map(); curConf.OK() {
			return curConf
		}
	}

	mainConf := utils.AsMap(utils.AnyMap{"job_name": "benchmark"})
	p.Check(p.Conf().Set("scrape_configs", scrapeConfigs.Append(mainConf)).GetError())
	return mainConf
}

func (p *PrometheusMaterial) GetStaticConfig(group string) *utils.Map {
	mainConf := p.MainScrapeConfig()
	staticConfigs := mainConf.SetDefaultList("static_configs")
	for i := 0; i < staticConfigs.Len(); i++ {
		s := staticConfigs.Get(i).Map()
		g, err := s.Get("labels").Map().Get("group").String()
		if err == nil && strings.EqualFold(strings.TrimSpace(g), group) {
			return s
		}
	}

	// group was not found
	staticConf := utils.AsMap(utils.AnyMap{"labels": utils.AnyMap{"group": group}})
	p.Check(mainConf.Set("static_configs", staticConfigs.Append(staticConf)).GetError())
	return staticConf
}

func (p *PrometheusMaterial) AddTarget(group string, target string) {
	staticConf := p.GetStaticConfig(group)
	curTargets := staticConf.Get("targets").List()
	curTargets = curTargets.Append(target)
	p.Check(curTargets.GetError())
	p.Check(staticConf.Set("targets", curTargets).GetError())
}

func (p *PrometheusMaterial) PrintConf() {
	b, err := p.Conf().Marshal()
	p.Check(err)
	fmt.Println(string(b))
}

func (p *PrometheusMaterial) Generate() {
	for _, node := range p.material.AllNodes() {
		p.AddTarget("nodes", node.PrometheusTargetAddress())
	}

	for _, worker := range p.material.AllWorkers() {
		p.AddTarget("workload", worker.PrometheusTargetAddress())
	}

	p.writeConfigFile(p.path)
}

func (p *PrometheusMaterial) Run() {
	p.Check(syscall.Exec(
		"/bin/prometheus",
		[]string{
			fmt.Sprintf("--config.file=\"%s\"", p.path),
			fmt.Sprintf("--storage.tsdb.path=\"%s\"",
				filepath.Join(p.material.config.Path.Metrics, "prometheus"),
			),
			fmt.Sprintf("--web.listen-address=%s", p.material.config.Prometheus.ListenAddress),
		},
		os.Environ(),
	))
}
