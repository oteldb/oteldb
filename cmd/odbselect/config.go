package main

import (
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/figureout"

	"github.com/oteldb/oteldb/internal/config"
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
	// ShutdownTimeout bounds how long in-flight queries are given to finish. Zero ⇒ 30s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
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

// describeConfig registers every field of [Config], which is what makes a key no field claims a
// startup error rather than a setting that looks applied and is not.
func describeConfig(c *Config, s *figureout.Schema[Config]) {
	figureout.Group(s, "cluster", func(s *figureout.Schema[Config]) {
		config.DescribeCluster(s, &c.Cluster)
	})
	figureout.Group(s, "prometheus", func(s *figureout.Schema[Config]) {
		config.DescribePrometheus(s, &c.Prometheus)
	})
	figureout.Group(s, "loki", func(s *figureout.Schema[Config]) {
		config.DescribeLoki(s, &c.Loki)
	})
	figureout.Group(s, "tempo", func(s *figureout.Schema[Config]) {
		config.DescribeTempo(s, &c.Tempo)
	})
	figureout.Group(s, "pyroscope", func(s *figureout.Schema[Config]) {
		config.DescribePyroscope(s, &c.Pyroscope)
	})
	figureout.Group(s, "health", func(s *figureout.Schema[Config]) {
		config.DescribeHealthCheck(s, &c.Health)
	})
	figureout.Value(s, &c.ShutdownTimeout, "shutdown_timeout")
}

// descriptor compiles the description once, on the path that can report a failure and exit.
var descriptor = sync.OnceValues(func() (*figureout.Descriptor[Config], error) {
	return config.Descriptor(describeConfig)
})

// loadConfig reads the config file, falling back to odbselect.yml.
func loadConfig(name string) (Config, error) {
	d, err := descriptor()
	if err != nil {
		return Config{}, errors.Wrap(err, "describe config")
	}

	cfg, _, err := config.Resolve(d, name, config.LoadOptions{Fallback: "odbselect.yml"})
	cfg.setDefaults()

	return cfg, err
}
