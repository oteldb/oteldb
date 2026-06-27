package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/yaml"
	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
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
	}()

	if name == "" {
		name = "oteldb.yml"
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
	Storage StorageConfig `json:"storage" yaml:"storage"`

	Tempo       TempoConfig       `json:"tempo" yaml:"tempo"`
	Prometheus  PrometheusConfig  `json:"prometheus" yaml:"prometheus"`
	Loki        LokiConfig        `json:"loki" yaml:"loki"`
	Pyroscope   PyroscopeConfig   `json:"pyroscope" yaml:"pyroscope"`
	HealthCheck HealthCheckConfig `json:"health_check" yaml:"health_check"`

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

// StorageConfig configures the embedded storage engine (used when
// [Config.MetricsBackend] is "storage").
type StorageConfig struct {
	// Backend is the engine backend: "memory" (default, ephemeral) or "file".
	Backend string `json:"backend" yaml:"backend"`
	// Dir is the data directory for the file backend (parts and WAL).
	Dir string `json:"dir" yaml:"dir"`
	// FlushInterval is the max age of unflushed head data before it is flushed to a part.
	// Zero uses the engine default. Ignored for the ephemeral memory backend.
	FlushInterval time.Duration `json:"flush_interval" yaml:"flush_interval"`
	// LogQueryParallelism enables concurrent materialization of LogQL query results across up to
	// this many workers. Zero or one (default) keeps the sequential path. Opt-in.
	LogQueryParallelism int `json:"log_query_parallelism" yaml:"log_query_parallelism"`
	// Cluster, when set with a non-empty Etcd endpoint list, joins this node to a storage cluster:
	// nodes coordinate through etcd, form a rendezvous-hash ring, and replicate writes across each
	// other's local backends. Unset (or empty Etcd) ⇒ a single-node engine.
	Cluster *StorageClusterConfig `json:"cluster" yaml:"cluster"`
}

// StorageClusterConfig configures the embedded storage engine's distribution layer (the shared-
// nothing cluster: each node keeps its shards on its own local backend and replicates them to RF
// peers). It maps onto storage's cluster.Config.
type StorageClusterConfig struct {
	// Etcd is the etcd endpoint list used for membership coordination. Enables cluster mode.
	Etcd []string `json:"etcd" yaml:"etcd"`
	// ID is this node's stable ring identity. Empty ⇒ the OS hostname.
	ID string `json:"id" yaml:"id"`
	// Zone is this node's failure domain (replicas are spread across zones). Optional.
	Zone string `json:"zone" yaml:"zone"`
	// Addr is the host:port peers use to reach this node's replication server. Empty ⇒
	// "<ID or hostname>:<Port>".
	Addr string `json:"addr" yaml:"addr"`
	// Port is the replication-server port used when Addr is derived from the hostname. Zero ⇒ 7946.
	Port int `json:"port" yaml:"port"`
	// RF is the replication factor (replicas per write). Zero ⇒ the storage default (3).
	RF int `json:"rf" yaml:"rf"`
	// ShardsPerTenant splits each tenant's metric series across this many independently-placed
	// shards. Zero or one ⇒ a single shard per tenant.
	ShardsPerTenant int `json:"shards_per_tenant" yaml:"shards_per_tenant"`
	// Root is the etcd key prefix for this cluster's state. Empty ⇒ "/oteldb".
	Root string `json:"root" yaml:"root"`
}

func (cfg *StorageConfig) setDefaults() {
	if cfg.Backend == "" {
		cfg.Backend = "memory"
	}
}

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
	cfg.Storage.setDefaults()
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

// TempoConfig is Tempo API config.
type TempoConfig struct {
	Bind string       `json:"bind" yaml:"bind"`
	Auth []AuthConfig `json:"auth" yaml:"auth"`
}

func (cfg *TempoConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3200"
	}
}

// PyroscopeConfig is Pyroscope API config.
type PyroscopeConfig struct {
	Bind string       `json:"bind" yaml:"bind"`
	Auth []AuthConfig `json:"auth" yaml:"auth"`
}

func (cfg *PyroscopeConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":4040"
	}
}

// PrometheusConfig is Prometheus API config.
type PrometheusConfig struct {
	Bind string       `json:"bind" yaml:"bind"`
	Auth []AuthConfig `json:"auth" yaml:"auth"`

	MaxSamples           int           `json:"max_samples" yaml:"max_samples"`
	MaxTimeseries        int           `json:"max_timeseries" yaml:"max_timeseries"`
	Timeout              time.Duration `json:"timeout" yaml:"timeout"`
	LookbackDelta        time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
	EnableAtModifier     bool          `json:"enable_at_modifier" yaml:"enable_at_modifier"`
	EnableNegativeOffset *bool         `json:"enable_negative_offset" yaml:"enable_negative_offset"`
	EnablePerStepStats   bool          `json:"enable_per_step_stats" yaml:"enable_per_step_stats"`

	// DisableRateOffloading disables PromQL rate offloading.
	DisableRateOffloading bool `json:"disable_rate_offloading" yaml:"disable_rate_offloading"`
	// DisableMetricOffloading disables all PromQL offloading.
	DisableMetricOffloading bool `json:"disable_metric_offloading" yaml:"disable_metric_offloading"`

	Cache MetricsCacheConfig `json:"cache" yaml:"cache"`
}

// MetricsCacheConfig is metrics cache config.
type MetricsCacheConfig struct {
	MaxBytes  xbytes.Bytes  `json:"max_bytes" yaml:"max_bytes"`
	SafetyLag time.Duration `json:"safety_lag" yaml:"safety_lag"`
}

func (cfg *PrometheusConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":9090"
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 1_000_000
	}
	if cfg.MaxTimeseries == 0 {
		cfg.MaxTimeseries = 1_000_000
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	setBool := func(p **bool, defaultValue bool) {
		if *p == nil {
			*p = &defaultValue
		}
	}
	setBool(&cfg.EnableNegativeOffset, true)
}

// LokiConfig is Loki API config.
type LokiConfig struct {
	Bind             string       `json:"bind" yaml:"bind"`
	Auth             []AuthConfig `json:"auth" yaml:"auth"`
	DrilldownEnabled bool         `json:"drilldown_enabled" yaml:"drilldown_enabled"`

	LookbackDelta time.Duration `json:"lookback_delta" yaml:"lookback_delta"`

	// MaxSampleRows defines max number of log rows a sample query
	// (count_over_time, rate, bytes_over_time, etc.) is allowed to fetch.
	MaxSampleRows int `json:"max_sample_rows" yaml:"max_sample_rows"`
	// MaxSampleResultBytes defines max number of result bytes a sample
	// query is allowed to fetch from ClickHouse.
	MaxSampleResultBytes xbytes.Bytes `json:"max_sample_result_bytes" yaml:"max_sample_result_bytes"`
}

func (cfg *LokiConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3100"
	}
	if cfg.MaxSampleRows == 0 {
		cfg.MaxSampleRows = 1_000_000
	}
	if cfg.MaxSampleResultBytes == 0 {
		cfg.MaxSampleResultBytes = 256 * 1024 * 1024 // 256 MiB
	}
}

// HealthCheckConfig is health check config.
type HealthCheckConfig struct {
	Bind string       `json:"bind" yaml:"bind"`
	Auth []AuthConfig `json:"auth" yaml:"auth"`
}

func (cfg *HealthCheckConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":13133"
	}
}

// AuthType defines authentication method type.
type AuthType string

const (
	AuthTypeNone        AuthType = "none"
	AuthTypeBasic       AuthType = "basicauth"
	AuthTypeBearerToken AuthType = "bearertoken"
)

// IsValid checks if auth type is valid.
func (t AuthType) IsValid() bool {
	switch t {
	case AuthTypeNone, AuthTypeBasic, AuthTypeBearerToken:
		return true
	default:
		return false
	}
}

// AuthConfig is authentication config.
type AuthConfig struct {
	Type   AuthType                         `json:"type" yaml:"type"`
	Tokens []httpmiddleware.Token           `json:"tokens" yaml:"tokens"`
	Users  []httpmiddleware.UserCredentials `json:"users" yaml:"users"`
}

func (cfg *AuthConfig) setDefaults() {
	if cfg.Type == "" {
		cfg.Type = AuthTypeNone
	}
}
