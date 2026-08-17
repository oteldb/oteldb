package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
)

// Config is the odbselect configuration.
type Config struct {
	// Cluster locates the storage cluster reads are routed through.
	Cluster ClusterConfig `json:"cluster" yaml:"cluster"`
	// Prometheus configures the PromQL API.
	Prometheus PrometheusConfig `json:"prometheus" yaml:"prometheus"`
	// Loki configures the LogQL API.
	Loki APIConfig `json:"loki" yaml:"loki"`
	// Tempo configures the TraceQL API.
	Tempo APIConfig `json:"tempo" yaml:"tempo"`
	// Pyroscope configures the profiles API.
	Pyroscope APIConfig `json:"pyroscope" yaml:"pyroscope"`
	// Health configures the health/readiness listener.
	Health APIConfig `json:"health" yaml:"health"`
	// ShutdownTimeout bounds how long in-flight queries are given to finish. Zero ⇒ 30s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

// ClusterConfig points odbselect at the ring. odbselect joins nothing and stores nothing: it
// follows membership read-only and reads each shard from one of that shard's owners.
//
// Every field here must match what the storage nodes are configured with. A mismatched
// ShardsPerTenant or RF does not fail — it resolves a different owner set than the nodes do, and
// reads look where the data is not.
type ClusterConfig struct {
	// Etcd is the endpoint list the cluster coordinates membership through. Required.
	Etcd []string `json:"etcd" yaml:"etcd"`
	// Root is the etcd key prefix for the cluster's state. Empty ⇒ "/oteldb".
	Root string `json:"root" yaml:"root"`
	// RF is the replication factor. Zero ⇒ 3.
	RF int `json:"rf" yaml:"rf"`
	// ShardsPerTenant is how many shards each tenant's data is split into. Zero or one ⇒ the tenant
	// is the shard.
	ShardsPerTenant int `json:"shards_per_tenant" yaml:"shards_per_tenant"`
	// DialTimeout bounds the initial etcd connection. Zero ⇒ 5s.
	DialTimeout time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
}

// APIConfig configures one query API listener. A bind of "-" disables the API.
type APIConfig struct {
	Bind string `json:"bind" yaml:"bind"`
}

// enabled reports whether the API should be served.
func (cfg APIConfig) enabled() bool { return cfg.Bind != "-" }

// PrometheusConfig configures the PromQL API and engine.
type PrometheusConfig struct {
	APIConfig `json:",inline" yaml:",inline"`

	// MaxSamples caps the samples one query may load. Zero ⇒ Prometheus' own default.
	MaxSamples int `json:"max_samples" yaml:"max_samples"`
	// Timeout bounds one query's evaluation. Zero ⇒ 1m.
	Timeout time.Duration `json:"timeout" yaml:"timeout"`
	// LookbackDelta is how far back an instant query looks for a sample. Zero ⇒ the engine default.
	LookbackDelta time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
	// EnableAtModifier allows the @ modifier.
	EnableAtModifier bool `json:"enable_at_modifier" yaml:"enable_at_modifier"`
	// EnableNegativeOffset allows a negative offset. Nil ⇒ true.
	EnableNegativeOffset *bool `json:"enable_negative_offset" yaml:"enable_negative_offset"`
	// EnablePerStepStats reports per-step evaluation statistics.
	EnablePerStepStats bool `json:"enable_per_step_stats" yaml:"enable_per_step_stats"`
}

func (cfg *Config) setDefaults() {
	if cfg.Prometheus.Bind == "" {
		cfg.Prometheus.Bind = ":9090"
	}
	if cfg.Prometheus.MaxSamples == 0 {
		cfg.Prometheus.MaxSamples = 50_000_000
	}
	if cfg.Prometheus.Timeout == 0 {
		cfg.Prometheus.Timeout = time.Minute
	}
	if cfg.Prometheus.EnableNegativeOffset == nil {
		enabled := true
		cfg.Prometheus.EnableNegativeOffset = &enabled
	}
	if cfg.Loki.Bind == "" {
		cfg.Loki.Bind = ":3100"
	}
	if cfg.Tempo.Bind == "" {
		cfg.Tempo.Bind = ":3200"
	}
	if cfg.Pyroscope.Bind == "" {
		cfg.Pyroscope.Bind = ":4040"
	}
	if cfg.Health.Bind == "" {
		cfg.Health.Bind = ":13133"
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
}

// validate refuses a config that would start but never work.
func (cfg *Config) validate() error {
	if len(cfg.Cluster.Etcd) == 0 {
		return errors.New("cluster.etcd is required: odbselect reads a cluster, it stores nothing itself")
	}

	for _, api := range []APIConfig{
		cfg.Prometheus.APIConfig, cfg.Loki, cfg.Tempo, cfg.Pyroscope,
	} {
		if api.enabled() {
			return nil
		}
	}

	return errors.New("every query API is disabled: odbselect would serve nothing")
}

// loadConfig reads the config file, falling back to odbselect.yml.
func loadConfig(name string) (cfg Config, _ error) {
	defer cfg.setDefaults()

	if name == "" {
		name = "odbselect.yml"
	}

	data, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
