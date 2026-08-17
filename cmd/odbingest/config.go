package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Config is the odbingest configuration.
type Config struct {
	// Cluster locates the storage cluster writes are routed into.
	Cluster ClusterConfig `json:"cluster" yaml:"cluster"`
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

// ClusterConfig points odbingest at the ring. odbingest joins nothing and stores nothing: it
// follows membership read-only and routes each shard's write to that shard's primary.
//
// Every field here must match what the storage nodes are configured with. A mismatched
// ShardsPerTenant or RF does not fail — it resolves a different owner set than the nodes do, and
// writes land where no read will look for them.
type ClusterConfig struct {
	// Etcd is the endpoint list the cluster coordinates membership through. Required.
	Etcd []string `json:"etcd" yaml:"etcd"`
	// Root is the etcd key prefix for the cluster's state. Empty ⇒ "/oteldb".
	Root string `json:"root" yaml:"root"`
	// RF is the replication factor. Zero ⇒ 3.
	RF int `json:"rf" yaml:"rf"`
	// ShardsPerTenant is how many shards each tenant's metric series are split into. Zero or one
	// ⇒ the tenant is the shard.
	ShardsPerTenant int `json:"shards_per_tenant" yaml:"shards_per_tenant"`
	// DialTimeout bounds the initial etcd connection. Zero ⇒ 5s.
	DialTimeout time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
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

// loadConfig reads the config file, falling back to odbingest.yml.
func loadConfig(name string) (cfg Config, _ error) {
	defer cfg.setDefaults()

	if name == "" {
		name = "odbingest.yml"
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
