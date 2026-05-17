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

	Tempo       TempoConfig       `json:"tempo" yaml:"tempo"`
	Prometheus  PrometheusConfig  `json:"prometheus" yaml:"prometheus"`
	Loki        LokiConfig        `json:"loki" yaml:"loki"`
	HealthCheck HealthCheckConfig `json:"health_check" yaml:"health_check"`

	// Auth is global auth config.
	Auth []AuthConfig `json:"auth" yaml:"auth"`

	// Whether if enable certain collector/inserter signals.
	CollectorSignals map[string]bool `json:"collector_signals" yaml:"collector_signals"`

	// Collector is an otelcol config.
	Collector map[string]any `json:"otelcol" yaml:"otelcol"`

	// Multitenancy is the multitenancy config.
	Multitenancy MultitenancyConfig `json:"multitenancy" yaml:"multitenancy"`
}

func (cfg *Config) setDefaults() {
	if len(cfg.CollectorSignals) == 0 {
		cfg.CollectorSignals = map[string]bool{
			"metrics": true,
			"logs":    true,
		}
	}
	if cfg.Collector == nil {
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
				"pipelines": map[string]any{
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
				},
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
}

func (cfg *LokiConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3100"
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

// MultitenancyConfig is the multitenancy config.
type MultitenancyConfig struct {
	// Enabled indicates if multitenancy is enabled.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// TenantMapper configures how tenant IDs are resolved from resource attributes.
	TenantMapper TenantMapperConfig `json:"tenant_mapper" yaml:"tenant_mapper"`

	// Resolver configures the backend resolver for mapping tokens to decisions.
	Resolver ResolverConfig `json:"resolver" yaml:"resolver"`

	// DefaultRestrictions are the default query restrictions.
	DefaultRestrictions RestrictionsConfig `json:"default_restrictions" yaml:"default_restrictions"`
}

// TenantMapperConfig configures tenant ID resolution from resource attributes.
type TenantMapperConfig struct {
	// Tenants is a map of compound keys to tenant IDs for explicit mapping.
	// Keys are in the format "attr1:value1,attr2:value2" -> "tenant_id".
	Tenants map[string]string `json:"tenants" yaml:"tenants"`

	// Template is a template string for tenant ID resolution using {key} substitution.
	// Example: "{service.namespace}_{service.name}"
	Template string `json:"template" yaml:"template"`

	// DefaultTenant is the default tenant ID to use if no match is found.
	DefaultTenant string `json:"default_tenant" yaml:"default_tenant"`
}

// ResolverConfig configures the backend resolver.
type ResolverConfig struct {
	// Type is the resolver type (e.g., "static", "http").
	Type string `json:"type" yaml:"type"`

	// Static contains static token-to-tenant mappings.
	Static []StaticTokenConfig `json:"static" yaml:"static"`

	// HTTP configures the HTTP callback resolver.
	HTTP HTTPResolverConfig `json:"http" yaml:"http"`
}

// StaticTokenConfig defines a static token mapping.
type StaticTokenConfig struct {
	// Token is the credential value.
	Token string `json:"token" yaml:"token"`
	// ReadTenantIDs are the tenants this token can query.
	ReadTenantIDs []string `json:"read_tenant_ids" yaml:"read_tenant_ids"`
	// WriteTenantIDs are the tenants this token can write for.
	WriteTenantIDs []string `json:"write_tenant_ids" yaml:"write_tenant_ids"`
	// ReadResourceSelectors are mandatory resource filters for queries.
	ReadResourceSelectors []ResourceSelectorConfig `json:"read_resource_selectors" yaml:"read_resource_selectors"`
	// Username is the informational username.
	Username string `json:"username" yaml:"username"`
	// QuotaKey is the ClickHouse quota key.
	QuotaKey string `json:"quota_key" yaml:"quota_key"`
}

// ResourceSelectorConfig defines a resource selector.
type ResourceSelectorConfig struct {
	Key   string `json:"key" yaml:"key"`
	Op    string `json:"op" yaml:"op"`
	Value string `json:"value" yaml:"value"`
}

// HTTPResolverConfig configures the HTTP callback resolver.
type HTTPResolverConfig struct {
	// URL is the callback URL.
	URL string `json:"url" yaml:"url"`
	// Timeout is the callback timeout.
	Timeout time.Duration `json:"timeout" yaml:"timeout"`
	// CacheTTL is the cache TTL.
	CacheTTL time.Duration `json:"cache_ttl" yaml:"cache_ttl"`
	// CredentialHeader is the header name to extract the credential from.
	CredentialHeader string `json:"credential_header" yaml:"credential_header"`
}

// RestrictionsConfig configures query restrictions.
type RestrictionsConfig struct {
	// MaxMemoryUsageBytes is the maximum memory usage in bytes.
	MaxMemoryUsageBytes uint64 `json:"max_memory_usage_bytes" yaml:"max_memory_usage_bytes"`
	// MaxExecutionTimeMS is the maximum execution time in milliseconds.
	MaxExecutionTimeMS int64 `json:"max_execution_time_ms" yaml:"max_execution_time_ms"`
	// MaxResultRows is the maximum number of result rows.
	MaxResultRows uint64 `json:"max_result_rows" yaml:"max_result_rows"`
}
