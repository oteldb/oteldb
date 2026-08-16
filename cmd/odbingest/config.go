package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/yaml"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// Config is the odbingest configuration.
type Config struct {
	// Storage configures the embedded storage engine data is written into. It is the same block
	// oteldb takes under `storage:`, so an ingester and a querier can share one config file.
	Storage storagebackend.Config `json:"storage" yaml:"storage"`
	// RemoteWrite configures the Prometheus remote write endpoint.
	RemoteWrite RemoteWriteConfig `json:"prometheus_remote_write" yaml:"prometheus_remote_write"`
}

// RemoteWriteConfig configures the Prometheus remote write ingest endpoint.
type RemoteWriteConfig struct {
	// Bind is the listen address. Empty ⇒ ":19291", the port oteldb's remote write receiver uses,
	// so a sender moves over without reconfiguration.
	Bind string `json:"bind" yaml:"bind"`
	// Path is the route the write endpoint is mounted at. Empty ⇒ "/", which accepts a write at
	// any path (what oteldb's receiver does today). Health probes keep their own paths regardless.
	Path string `json:"path" yaml:"path"`
	// TimeThreshold drops points older than it. Zero ⇒ 24h.
	TimeThreshold time.Duration `json:"time_threshold" yaml:"time_threshold"`
	// MaxBodyBytes limits the compressed request body. Zero ⇒ 64 MiB.
	MaxBodyBytes xbytes.Bytes `json:"max_body_bytes" yaml:"max_body_bytes"`
	// MaxDecodedBytes limits what a request body may decompress to. Zero ⇒ 256 MiB.
	MaxDecodedBytes xbytes.Bytes `json:"max_decoded_bytes" yaml:"max_decoded_bytes"`
	// ReadHeaderTimeout bounds how long a client may take to send its headers. Zero ⇒ 5s.
	ReadHeaderTimeout time.Duration `json:"read_header_timeout" yaml:"read_header_timeout"`
	// ShutdownTimeout bounds how long in-flight writes are given to finish on shutdown, before the
	// engine is flushed and closed. Zero ⇒ 15s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

func (cfg *Config) setDefaults() {
	cfg.Storage.SetDefaults()

	rw := &cfg.RemoteWrite
	if rw.Bind == "" {
		rw.Bind = ":19291"
	}
	if rw.Path == "" {
		rw.Path = "/"
	}
	if rw.ReadHeaderTimeout == 0 {
		rw.ReadHeaderTimeout = 5 * time.Second
	}
	if rw.ShutdownTimeout == 0 {
		rw.ShutdownTimeout = 15 * time.Second
	}
}

// loadConfig reads the config file, falling back to odbingest.yml and, when that is absent too, to
// the defaults (an ephemeral in-memory engine, useful for a smoke test).
func loadConfig(name string) (cfg Config, _ error) {
	defer cfg.setDefaults()

	if name == "" {
		name = "odbingest.yml"
		if _, err := os.Stat(name); err != nil {
			return cfg, nil
		}
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
