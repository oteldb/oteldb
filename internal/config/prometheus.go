package config

import (
	"time"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Prometheus is Prometheus API config.
type Prometheus struct {
	Listener `json:",inline" yaml:",inline"`

	// MaxSamples caps the cumulative samples a query reads. It defaults to 50 million.
	MaxSamples           int           `json:"max_samples" yaml:"max_samples"`
	MaxTimeseries        int           `json:"max_timeseries" yaml:"max_timeseries"`
	Timeout              time.Duration `json:"timeout" yaml:"timeout"`
	LookbackDelta        time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
	EnableAtModifier     bool          `json:"enable_at_modifier" yaml:"enable_at_modifier"`
	EnableNegativeOffset *bool         `json:"enable_negative_offset" yaml:"enable_negative_offset"`

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
	cfg.Listener.setDefaults(":9090")
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
