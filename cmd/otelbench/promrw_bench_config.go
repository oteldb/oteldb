package main

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v2"
)

func newConfig(targetsCount int, scrapeInterval time.Duration, targetAddr string) *config {
	// https://github.com/VictoriaMetrics/prometheus-benchmark/blob/50c5891/services/vmagent-config-updater/main.go#L72-L94
	scs := make([]*staticConfig, 0, targetsCount)
	for i := range targetsCount {
		scs = append(scs, &staticConfig{
			Targets: []string{targetAddr},
			Labels: map[string]string{
				"instance": fmt.Sprintf("host-%d", i),
				"revision": "r0",
			},
		})
	}
	return &config{
		Global: globalConfig{
			ScrapeInterval: scrapeInterval,
		},
		ScrapeConfigs: []*scrapeConfig{
			{
				JobName:       "node_exporter",
				StaticConfigs: scs,
				Path:          "/node",
			},
		},
	}
}

// config represents essential parts from Prometheus config defined at https://prometheus.io/docs/prometheus/latest/configuration/configuration/
type config struct {
	Global        globalConfig         `yaml:"global"`
	ScrapeConfigs []*scrapeConfig      `yaml:"scrape_configs,omitempty"`
	RemoteWrites  []*remoteWriteConfig `yaml:"remote_write,omitempty"`
}

func (c *config) marshalYAML() []byte {
	data, err := yaml.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

// globalConfig represents essential parts for `global` section of Prometheus config.
//
// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/
type globalConfig struct {
	ScrapeInterval time.Duration `yaml:"scrape_interval"`
}

// rapeConfig represents essential parts for `scrape_config` section of Prometheus config.
//
// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config
type scrapeConfig struct {
	JobName       string          `yaml:"job_name"`
	Path          string          `yaml:"metrics_path,omitempty"`
	StaticConfigs []*staticConfig `yaml:"static_configs"`
}

// staticConfig represents essential parts for `static_config` section of Prometheus config.
//
// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#static_config
type staticConfig struct {
	Targets []string          `yaml:"targets"`
	Labels  map[string]string `yaml:"labels"`
}

// remoteWriteConfig represents essential parts for `remote_write` section of Prometheus config.
//
// https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write
type remoteWriteConfig struct {
	URL      string                     `yaml:"url"`
	Name     string                     `yaml:"name,omitempty"`
	Metadata *remoteWriteMetadataConfig `yaml:"metadata_config,omitempty"`
}

type remoteWriteMetadataConfig struct {
	Send         bool          `yaml:"send"`
	SendInterval time.Duration `yaml:"send_interval,omitempty"`
}
