package main

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/xbytes"
)

func loadConfig(name string) (cfg Config, _ error) {
	defer func() {
		// Environment variable has higher precedence.
		if dsn := os.Getenv("CH_DSN"); dsn != "" {
			cfg.DSN = dsn
		}
		if backend := os.Getenv("METRICS_BACKEND"); backend != "" {
			cfg.MetricsBackend = backend
		}
		// Per-node storage cluster identity is supplied via the environment so a whole
		// deployment can share one config file/ConfigMap while each replica contributes its
		// own stable ring id and peer-reachable address (e.g. a StatefulSet pod's name and
		// its headless-service FQDN). This is what the Kubernetes operator injects per pod.
		ensureCluster := func() {
			if cfg.Storage.Cluster == nil {
				cfg.Storage.Cluster = &storagebackend.ClusterConfig{}
			}
		}
		if v := os.Getenv("OTELDB_CLUSTER_ID"); v != "" {
			ensureCluster()
			cfg.Storage.Cluster.ID = v
		}
		if v := os.Getenv("OTELDB_CLUSTER_ADDR"); v != "" {
			ensureCluster()
			cfg.Storage.Cluster.Addr = v
		}
		if v := os.Getenv("OTELDB_CLUSTER_ZONE"); v != "" {
			ensureCluster()
			cfg.Storage.Cluster.Zone = v
		}
		// The failure domain may instead be discovered at runtime (e.g. an init container reads
		// the node's topology zone label into a file), since it is not known when the shared
		// config is rendered. A non-empty OTELDB_CLUSTER_ZONE takes precedence.
		if p := os.Getenv("OTELDB_CLUSTER_ZONE_FILE"); p != "" && (cfg.Storage.Cluster == nil || cfg.Storage.Cluster.Zone == "") {
			if b, err := os.ReadFile(filepath.Clean(p)); err == nil {
				if z := strings.TrimSpace(string(b)); z != "" {
					ensureCluster()
					cfg.Storage.Cluster.Zone = z
				}
			}
		}
		if v := os.Getenv("OTELDB_CLUSTER_ETCD"); v != "" {
			ensureCluster()
			cfg.Storage.Cluster.Etcd = strings.Split(v, ",")
		}
	}()

	return config.Load[Config](name, config.LoadOptions{
		Fallback: "oteldb.yml",
		Optional: true,
	})
}

// Config is the oteldb config.
type Config struct {
	DSN        string        `json:"dsn" yaml:"dsn"`
	TTL        time.Duration `json:"ttl" yaml:"ttl"`
	Cluster    string        `json:"cluster" yaml:"cluster"`
	Replicated bool          `json:"replicated" yaml:"replicated"`
	CHLogLevel zapcore.Level `json:"ch_log_level" yaml:"ch_log_level"`

	MaxResultRows    int           `json:"max_result_rows" yaml:"max_result_rows"`
	MaxResultBytes   xbytes.Bytes  `json:"max_result_bytes" yaml:"max_result_bytes"`
	MaxExecutionTime time.Duration `json:"max_execution_time" yaml:"max_execution_time"`

	// MetricsBackend selects the storage backend serving the metrics signal:
	// "clickhouse" (default) or "storage" (the embedded github.com/oteldb/storage engine).
	MetricsBackend string `json:"metrics_backend" yaml:"metrics_backend"`
	// TracesBackend selects the storage backend serving the traces signal:
	// "clickhouse" (default) or "storage".
	TracesBackend string `json:"traces_backend" yaml:"traces_backend"`
	// LogsBackend selects the storage backend serving the logs signal:
	// "clickhouse" (default) or "storage".
	LogsBackend string `json:"logs_backend" yaml:"logs_backend"`
	// ProfilesBackend selects the storage backend serving the profiles signal. Profiles have no
	// ClickHouse implementation, so this is empty (the Pyroscope API stays unregistered) unless
	// set to "storage" (the embedded github.com/oteldb/storage engine).
	ProfilesBackend string `json:"profiles_backend" yaml:"profiles_backend"`
	// Storage configures the embedded storage engine, used when a signal's backend is "storage".
	Storage storagebackend.Config `json:"storage" yaml:"storage"`

	Tempo       TempoConfig       `json:"tempo" yaml:"tempo"`
	Prometheus  PrometheusConfig  `json:"prometheus" yaml:"prometheus"`
	Loki        LokiConfig        `json:"loki" yaml:"loki"`
	Pyroscope   PyroscopeConfig   `json:"pyroscope" yaml:"pyroscope"`
	HealthCheck HealthCheckConfig `json:"health_check" yaml:"health_check"`
	Admin       AdminConfig       `json:"admin" yaml:"admin"`

	// Auth is global auth config.
	Auth []AuthConfig `json:"auth" yaml:"auth"`

	// Whether if enable certain collector/inserter signals.
	CollectorSignals map[string]bool `json:"collector_signals" yaml:"collector_signals"`

	// Collector is an otelcol config.
	Collector map[string]any `json:"otelcol" yaml:"otelcol"`
}

// Metrics backend identifiers for [Config.MetricsBackend].
const (
	// MetricsBackendClickHouse serves metrics from ClickHouse (the default).
	MetricsBackendClickHouse = "clickhouse"
	// MetricsBackendStorage serves metrics from the embedded github.com/oteldb/storage engine.
	MetricsBackendStorage = "storage"
)

// useEmbeddedStorage routes every signal to the embedded storage engine. It is the one-liner
// behind the --embedded flag, equivalent to setting metrics_backend/traces_backend/logs_backend/
// profiles_backend all to "storage" in the config.
func (cfg *Config) useEmbeddedStorage() {
	cfg.MetricsBackend = MetricsBackendStorage
	cfg.TracesBackend = MetricsBackendStorage
	cfg.LogsBackend = MetricsBackendStorage
	cfg.ProfilesBackend = MetricsBackendStorage
}

// needsClickHouse reports whether any queryable signal is still served by ClickHouse and therefore
// the ClickHouse storage (including the zero-config embedded ClickHouse) must be started. Profiles
// are excluded because they have no ClickHouse implementation. When this returns false (e.g. under
// --embedded), ClickHouse is skipped entirely and every signal is served from the embedded engine.
func (cfg *Config) needsClickHouse() bool {
	return cfg.MetricsBackend != MetricsBackendStorage ||
		cfg.TracesBackend != MetricsBackendStorage ||
		cfg.LogsBackend != MetricsBackendStorage
}

// usesStorageBackend reports whether any signal is served by the embedded storage engine.
func (cfg *Config) usesStorageBackend() bool {
	return cfg.MetricsBackend == MetricsBackendStorage ||
		cfg.TracesBackend == MetricsBackendStorage ||
		cfg.LogsBackend == MetricsBackendStorage ||
		cfg.ProfilesBackend == MetricsBackendStorage
}

func (cfg *Config) setDefaults() {
	if cfg.MetricsBackend == "" {
		cfg.MetricsBackend = MetricsBackendClickHouse
	}
	if cfg.TracesBackend == "" {
		cfg.TracesBackend = MetricsBackendClickHouse
	}
	if cfg.LogsBackend == "" {
		cfg.LogsBackend = MetricsBackendClickHouse
	}
	cfg.Storage.SetDefaults()
	if len(cfg.CollectorSignals) == 0 {
		cfg.CollectorSignals = map[string]bool{
			"metrics": true,
			"logs":    true,
		}
	}
	if cfg.Collector == nil {
		pipelines := map[string]any{
			"traces": map[string]any{
				"receivers": []string{"otlp"},
				"exporters": []string{"oteldbexporter"},
			},
			"metrics": map[string]any{
				"receivers": []string{"otlp", "prometheusremotewrite"},
				"exporters": []string{"oteldbexporter"},
			},
			"logs": map[string]any{
				"receivers": []string{"otlp"},
				"exporters": []string{"oteldbexporter"},
			},
		}
		// The profiles signal is experimental and only representable by the embedded storage
		// engine, so its pipeline is enabled only when profiles are served from storage.
		if cfg.ProfilesBackend == MetricsBackendStorage {
			pipelines["profiles"] = map[string]any{
				"receivers": []string{"otlp"},
				"exporters": []string{"oteldbexporter"},
			}
		}
		cfg.Collector = map[string]any{
			"receivers": map[string]any{
				"otlp": map[string]any{
					"protocols": map[string]any{
						"grpc": map[string]any{
							"endpoint":              "0.0.0.0:4317",
							"max_recv_msg_size_mib": 512,
						},
						"http": map[string]any{
							"endpoint": "0.0.0.0:4318",
						},
					},
				},
				"prometheusremotewrite": map[string]any{},
			},
			"exporters": map[string]any{
				"oteldbexporter": map[string]any{
					"dsn": cfg.DSN,
				},
			},
			"service": map[string]any{
				"pipelines": pipelines,
			},
		}
	}
}

// Per-signal config blocks, shared with the role binaries.
type (
	TempoConfig        = config.Tempo
	PyroscopeConfig    = config.Pyroscope
	PrometheusConfig   = config.Prometheus
	MetricsCacheConfig = config.MetricsCache
	LokiConfig         = config.Loki
	AdminConfig        = config.Admin
	HealthCheckConfig  = config.HealthCheck
	AuthConfig         = config.Auth
	AuthType           = config.AuthType
)

const (
	AuthTypeNone        = config.AuthTypeNone
	AuthTypeBasic       = config.AuthTypeBasic
	AuthTypeBearerToken = config.AuthTypeBearerToken
)
