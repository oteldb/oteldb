package main

import (
	"slices"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// Config is the odbselect configuration.
type Config struct {
	// Cluster locates the storage cluster reads are routed through.
	Cluster config.Cluster `json:"cluster" yaml:"cluster"`
	// Prometheus configures the PromQL API.
	Prometheus config.Prometheus `json:"prometheus" yaml:"prometheus"`
	// Loki configures the LogQL API.
	Loki config.Loki `json:"loki" yaml:"loki"`
	// Tempo configures the TraceQL API.
	Tempo config.Tempo `json:"tempo" yaml:"tempo"`
	// Pyroscope configures the profiles API.
	Pyroscope config.Pyroscope `json:"pyroscope" yaml:"pyroscope"`
	// Health configures the health/readiness listener.
	Health config.HealthCheck `json:"health" yaml:"health"`
	// MaxQueryBytes caps what one query may hold before it is refused. An aggregator needs its own
	// bound: it holds every shard owner's answer at once to merge them, so the owners' limits do not
	// add up to one here. Unset ⇒ a share of the detected process budget; 0 ⇒ unbounded.
	MaxQueryBytes *xbytes.Bytes `json:"max_query_bytes" yaml:"max_query_bytes"`
	// ShutdownTimeout bounds how long in-flight queries are given to finish. Zero ⇒ 30s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

// maxQueryBytes resolves the per-query read bound for [clusterquery.New], inverting the polarity of
// the config so it reads like the storage engine's cache settings: unset means "size it from the
// process budget" (0) and an explicit 0 means "unbounded" (negative).
func (cfg *Config) maxQueryBytes() int64 {
	if cfg.MaxQueryBytes == nil {
		return 0
	}

	if n := int64(*cfg.MaxQueryBytes); n > 0 {
		return n
	}

	return -1
}

// enabled reports whether an API with this bind should be served. A bind of "-" disables it, which
// is how odbselect drops a signal it does not want to answer for; cmd/oteldb instead has no querier
// to serve it from.
func enabled(bind string) bool { return bind != "-" }

func (cfg *Config) setDefaults() {
	for _, block := range []config.Defaulter{
		&cfg.Prometheus, &cfg.Loki, &cfg.Tempo, &cfg.Pyroscope, &cfg.Health,
	} {
		block.SetDefaults()
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

	if slices.ContainsFunc([]string{
		cfg.Prometheus.Bind, cfg.Loki.Bind, cfg.Tempo.Bind, cfg.Pyroscope.Bind,
	}, enabled) {
		return nil
	}

	return errors.New("every query API is disabled: odbselect would serve nothing")
}

// loadConfig reads the config file, falling back to odbselect.yml.
func loadConfig(name string) (Config, error) {
	cfg, err := config.Load[Config](name, config.LoadOptions{Fallback: "odbselect.yml"})
	cfg.setDefaults()

	return cfg, err
}
