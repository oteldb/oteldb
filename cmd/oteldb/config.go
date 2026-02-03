package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/yaml"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/httpmiddleware"
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
