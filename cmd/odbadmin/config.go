package main

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/config"
)

// Config is the odbadmin configuration.
type Config struct {
	// Cluster locates the storage cluster whose nodes are aggregated.
	Cluster config.Cluster `json:"cluster" yaml:"cluster"`
	// Nodes configures how each member node's admin API is reached.
	Nodes Nodes `json:"nodes" yaml:"nodes"`
	// Admin configures the aggregated admin API and web UI listener.
	Admin config.Admin `json:"admin" yaml:"admin"`
	// Health configures the health/readiness listener.
	Health config.HealthCheck `json:"health" yaml:"health"`
	// ShutdownTimeout bounds how long in-flight requests are given to finish. Zero ⇒ 30s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

// Nodes describes how odbadmin reaches the storage nodes' own admin APIs.
//
// A ring member advertises the address its peers use for cluster RPCs, not its admin API, so the
// endpoint is that address's host with the port below. Every node is assumed to serve its admin API
// on the same port, which is what a homogeneous storage deployment does.
type Nodes struct {
	// Scheme is http or https. Empty ⇒ http.
	Scheme string `json:"scheme" yaml:"scheme"`
	// Port is the admin API port every storage node serves on. Zero ⇒ 8090.
	Port int `json:"port" yaml:"port"`
	// Timeout bounds one node's answer. Zero ⇒ 10s. It is per node rather than per request, so one
	// unresponsive node costs the report its own share and nothing more.
	Timeout time.Duration `json:"timeout" yaml:"timeout"`
}

// SetDefaults implements [config.Defaulter].
func (cfg *Nodes) SetDefaults() {
	if cfg.Scheme == "" {
		cfg.Scheme = "http"
	}
	if cfg.Port == 0 {
		cfg.Port = defaultNodeAdminPort
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
}

// defaultNodeAdminPort is the port cmd/oteldb serves its admin API on.
const defaultNodeAdminPort = 8090

func (cfg *Config) setDefaults() {
	for _, block := range []config.Defaulter{&cfg.Nodes, &cfg.Admin, &cfg.Health} {
		block.SetDefaults()
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
}

// validate refuses a config that would start but never work.
func (cfg *Config) validate() error {
	if len(cfg.Cluster.Etcd) == 0 {
		return errors.New("cluster.etcd is required: odbadmin reports on a cluster, it stores nothing itself")
	}
	if cfg.Admin.Bind == "" || cfg.Admin.Bind == "-" {
		return errors.New("admin.bind is required: the admin API is the only thing odbadmin serves")
	}

	return nil
}

// loadConfig reads the config file, falling back to odbadmin.yml.
func loadConfig(name string) (Config, error) {
	cfg, err := config.Load[Config](name, config.LoadOptions{Fallback: "odbadmin.yml"})
	cfg.setDefaults()

	return cfg, err
}
