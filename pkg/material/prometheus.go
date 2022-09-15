package material

import (
	"fmt"
	"io/ioutil"

	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"gopkg.in/yaml.v3"
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
	var conf utils.AnyMap
	b, err := ioutil.ReadFile(configFilePath)
	p.Check(err)
	p.Check(yaml.Unmarshal(b, &conf))
	return utils.AsMap(conf)
}

func (p *PrometheusMaterial) writeConfigFile(configFilePath string) {
	b, err := yaml.Marshal(p.Conf().Any())
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
	p.Conf().Set("scrape_configs", scrapeConfigs.Append(mainConf))
	return mainConf
}

func (p *PrometheusMaterial) GetStaticConfig(group string) *utils.Map {
	mainConf := p.MainScrapeConfig()
	staticConfigs := mainConf.SetDefaultList("static_configs")
	for i := 0; i < staticConfigs.Len(); i++ {
		s := staticConfigs.Get(i).Map()
		if g, ok := s.Get("labels").Map().Get("group").String(); ok && g == group {
			return s
		}
	}

	// group was not found
	staticConf := utils.AsMap(utils.AnyMap{"labels": utils.AnyMap{"group": group}})
	mainConf.Set("static_configs", staticConfigs.Append(staticConf))
	return staticConf
}

func (p *PrometheusMaterial) AddTarget(group string, target string) {
	staticConf := p.GetStaticConfig(group)
	curTargets := staticConf.SetDefaultList("targets")
	staticConf.Set("targets", curTargets.Append(target))
}

func (p *PrometheusMaterial) PrintConf() {
	b, err := yaml.Marshal(p.Conf().Any())
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
