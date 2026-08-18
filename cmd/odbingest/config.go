package main

import (
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/figureout"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// Config is the odbingest configuration.
type Config struct {
	// Cluster locates the storage cluster writes are routed into.
	Cluster config.Cluster `json:"cluster" yaml:"cluster"`
	// RemoteWrite configures the Prometheus remote write endpoint.
	RemoteWrite RemoteWriteConfig `json:"prometheus_remote_write" yaml:"prometheus_remote_write"`
	// OTLP configures the OTLP/HTTP endpoints, which share the remote write listener.
	OTLP OTLPConfig `json:"otlp" yaml:"otlp"`
	// Tenant configures which tenant a write routes to. Empty ⇒ everything routes to the
	// "default" tenant, as it does with no tenancy configured at all.
	Tenant TenantConfig `json:"tenant" yaml:"tenant"`
}

// TenantConfig configures tenant resolution on the ingest path.
//
// The zero value routes every write to [cluster.DefaultTenant]. That is deliberate and must stay
// that way: a tenant picks the shard key, which picks the ring owner, so a deployment that starts
// resolving tenants differently writes where its existing reads do not look. Enabling a source
// here is a migration, not a config tweak.
//
// Sources compose narrowest-first — Header (a whole request) beats ResourceAttributes (one
// resource within it), which beats Default.
type TenantConfig struct {
	// Default is the tenant a write routes to when no source names one. Empty ⇒ "default".
	Default string `json:"default" yaml:"default"`
	// Header is the request header (and OTLP/gRPC metadata key) carrying the tenant. Empty ⇒ the
	// header is not read. "X-Scope-OrgID" is what Grafana-stack senders put it in.
	Header string `json:"header" yaml:"header"`
	// ResourceAttributes are the OTLP resource attribute keys read, in order, until one holds a
	// usable tenant. Empty ⇒ resource attributes are not read. "service.namespace" is the
	// OTel-native candidate.
	ResourceAttributes []string `json:"resource_attributes" yaml:"resource_attributes"`
	// Require refuses a request that does not carry Header, instead of routing it to Default.
	// Requires Header.
	Require bool `json:"require" yaml:"require"`
}

// OTLPConfig configures the OTLP/HTTP ingest endpoints. They are always served, at the paths the
// OTLP spec fixes (/v1/logs, /v1/traces, /v1/metrics, /v1/profiles).
type OTLPConfig struct {
	// GRPCBind is the OTLP/gRPC listen address. Empty ⇒ ":4317", the port every OTLP exporter
	// targets by default. "-" disables gRPC.
	GRPCBind string `json:"grpc_bind" yaml:"grpc_bind"`
	// MaxBodyBytes limits the request body. Zero ⇒ 64 MiB.
	MaxBodyBytes xbytes.Bytes `json:"max_body_bytes" yaml:"max_body_bytes"`
	// MaxDecodedBytes limits what a gzip body may expand to. Zero ⇒ 256 MiB.
	MaxDecodedBytes xbytes.Bytes `json:"max_decoded_bytes" yaml:"max_decoded_bytes"`
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
	// ShutdownTimeout bounds how long in-flight writes are given to finish on shutdown. Zero ⇒ 15s.
	ShutdownTimeout time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

func (cfg *Config) setDefaults() {
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

	if cfg.OTLP.GRPCBind == "" {
		cfg.OTLP.GRPCBind = ":4317"
	}
}

// validate refuses a config that would start but never work.
func (cfg *Config) validate() error {
	if len(cfg.Cluster.Etcd) == 0 {
		return errors.New("cluster.etcd is required: odbingest routes to a cluster, it stores nothing itself")
	}

	return nil
}

// describeConfig registers every field of [Config], which is what makes a key no field claims a
// startup error rather than a setting that looks applied and is not.
func describeConfig(c *Config, s *figureout.Schema[Config]) {
	figureout.Group(s, "cluster", func(s *figureout.Schema[Config]) {
		config.DescribeCluster(s, &c.Cluster)
	})
	figureout.Group(s, "prometheus_remote_write", func(s *figureout.Schema[Config]) {
		rw := &c.RemoteWrite
		figureout.Value(s, &rw.Bind, "bind")
		figureout.Value(s, &rw.Path, "path")
		figureout.Value(s, &rw.TimeThreshold, "time_threshold")
		figureout.Value(s, &rw.MaxBodyBytes, "max_body_bytes")
		figureout.Value(s, &rw.MaxDecodedBytes, "max_decoded_bytes")
		figureout.Value(s, &rw.ReadHeaderTimeout, "read_header_timeout")
		figureout.Value(s, &rw.ShutdownTimeout, "shutdown_timeout")
	})
	figureout.Group(s, "otlp", func(s *figureout.Schema[Config]) {
		figureout.Value(s, &c.OTLP.GRPCBind, "grpc_bind")
		figureout.Value(s, &c.OTLP.MaxBodyBytes, "max_body_bytes")
		figureout.Value(s, &c.OTLP.MaxDecodedBytes, "max_decoded_bytes")
	})
	figureout.Group(s, "tenant", func(s *figureout.Schema[Config]) {
		figureout.Value(s, &c.Tenant.Default, "default")
		figureout.Value(s, &c.Tenant.Header, "header")
		figureout.Value(s, &c.Tenant.ResourceAttributes, "resource_attributes")
		figureout.Value(s, &c.Tenant.Require, "require")
	})
}

// descriptor compiles the description once, on the path that can report a failure and exit.
var descriptor = sync.OnceValues(func() (*figureout.Descriptor[Config], error) {
	return config.Descriptor(describeConfig)
})

// loadConfig reads the config file, falling back to odbingest.yml.
func loadConfig(name string) (cfg Config, _ error) {
	defer cfg.setDefaults()

	d, err := descriptor()
	if err != nil {
		return cfg, errors.Wrap(err, "describe config")
	}

	cfg, _, err = config.Resolve(d, name, config.LoadOptions{Fallback: "odbingest.yml"})

	return cfg, err
}
