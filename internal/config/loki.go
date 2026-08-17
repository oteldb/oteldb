package config

import (
	"time"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Loki is Loki API config.
type Loki struct {
	Bind             string `json:"bind" yaml:"bind"`
	Auth             []Auth `json:"auth" yaml:"auth"`
	DrilldownEnabled bool   `json:"drilldown_enabled" yaml:"drilldown_enabled"`

	LookbackDelta time.Duration `json:"lookback_delta" yaml:"lookback_delta"`

	// MaxSampleRows defines max number of log rows a sample query
	// (count_over_time, rate, bytes_over_time, etc.) is allowed to fetch.
	MaxSampleRows int `json:"max_sample_rows" yaml:"max_sample_rows"`
	// MaxSampleResultBytes defines max number of result bytes a sample
	// query is allowed to fetch from ClickHouse.
	MaxSampleResultBytes xbytes.Bytes `json:"max_sample_result_bytes" yaml:"max_sample_result_bytes"`
}

// SetDefaults implements [Defaulter].
func (cfg *Loki) SetDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3100"
	}
	if cfg.MaxSampleRows == 0 {
		cfg.MaxSampleRows = 1_000_000
	}
	if cfg.MaxSampleResultBytes == 0 {
		cfg.MaxSampleResultBytes = 256 * 1024 * 1024 // 256 MiB.
	}
}
