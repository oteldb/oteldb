package config

import (
	"time"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Prometheus is Prometheus API config.
type Prometheus struct {
	Bind string `json:"bind" yaml:"bind"`
	Auth []Auth `json:"auth" yaml:"auth"`

	// MaxSamples caps the samples one query may load, defaulting to Prometheus' own
	// --query.max-samples. The two engines count it differently: the fork tracks samples resident
	// at once, scarecrow every sample read (docs/promql-engine.md, M11), because its columnar model
	// holds one series at a time and a peak gauge would never trip on a scan touching millions of
	// series. So the same number is stricter under scarecrow — size it for the total a query reads,
	// not for its result.
	MaxSamples           int           `json:"max_samples" yaml:"max_samples"`
	MaxTimeseries        int           `json:"max_timeseries" yaml:"max_timeseries"`
	Timeout              time.Duration `json:"timeout" yaml:"timeout"`
	LookbackDelta        time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
	EnableAtModifier     bool          `json:"enable_at_modifier" yaml:"enable_at_modifier"`
	EnableNegativeOffset *bool         `json:"enable_negative_offset" yaml:"enable_negative_offset"`
	EnablePerStepStats   bool          `json:"enable_per_step_stats" yaml:"enable_per_step_stats"`

	// EnableScarecrowEngine routes PromQL queries through internal/scarecrow (the native
	// series-major engine, docs/promql-engine.md) instead of the Thanos-fork engine
	// (internal/promql). It gets a native columnar Scanner (no per-sample copy/iterator boxing)
	// only when metrics are served from the embedded storage engine (metrics.backend: storage);
	// otherwise it falls back to scarecrow's generic storage.Queryable adapter, which is correct
	// but pays the same conversion cost the fork already does. MaxSamples and Timeout are enforced
	// (MaxSamples cumulatively, see its own doc); EnablePerStepStats is not. Experimental: corpus
	// coverage is partial
	// (see internal/scarecrow's unsupportedFiles), so unsupported query shapes error instead of
	// falling back to the fork.
	EnableScarecrowEngine bool `json:"enable_scarecrow_engine" yaml:"enable_scarecrow_engine"`

	// DisableRateOffloading disables PromQL rate offloading.
	DisableRateOffloading bool `json:"disable_rate_offloading" yaml:"disable_rate_offloading"`
	// DisableMetricOffloading disables all PromQL offloading.
	DisableMetricOffloading bool `json:"disable_metric_offloading" yaml:"disable_metric_offloading"`

	Cache MetricsCache `json:"cache" yaml:"cache"`
}

// MetricsCache is metrics cache config.
type MetricsCache struct {
	MaxBytes  xbytes.Bytes  `json:"max_bytes" yaml:"max_bytes"`
	SafetyLag time.Duration `json:"safety_lag" yaml:"safety_lag"`
}

// SetDefaults implements [Defaulter].
func (cfg *Prometheus) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":9090"
	}
	if cfg.MaxSamples == 0 {
		// Prometheus' own default. The previous 1M was 50x below it, which a single node-exporter
		// CPU panel exceeds: 256 series over 6h at a 5s scrape is 1.1M samples.
		cfg.MaxSamples = 50_000_000
	}
	if cfg.MaxTimeseries == 0 {
		cfg.MaxTimeseries = 1_000_000
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	if cfg.EnableNegativeOffset == nil {
		enabled := true
		cfg.EnableNegativeOffset = &enabled
	}
}
