// Package odbsafety contains shared log safety configuration.
package odbsafety

import (
	"time"

	"github.com/go-faster/errors"
)

const (
	// ModeConsume accepts all records and disables excess handling.
	ModeConsume = "consume"
	// ModeDrop drops excess records.
	ModeDrop = "drop"
	// ModeSample samples excess records.
	ModeSample = "sample"
	// ModeCompact compacts repeated excess records.
	ModeCompact = "compact"
	// ModeTruncate emits synthetic truncation records for excess records.
	ModeTruncate = "truncate"
)

// Config defines shared log safety options.
type Config struct {
	MaxRatePerSecond int     `mapstructure:"max_rate_per_second"`
	OnExcess         string  `mapstructure:"on_excess"`
	SampleRate       float64 `mapstructure:"sample_rate"`

	CompactWindow     time.Duration `mapstructure:"compact_window"`
	CompactThreshold  int           `mapstructure:"compact_threshold"`
	CompactMaxBuckets int           `mapstructure:"compact_max_buckets"`
	CompactKeyFields  []string      `mapstructure:"compact_key_fields"`
	TruncateThreshold int           `mapstructure:"truncate_threshold"`

	RedactFields []string `mapstructure:"redact_fields"`
}

// DefaultConfig returns shared log safety defaults.
func DefaultConfig() Config {
	return Config{
		OnExcess:          ModeConsume,
		SampleRate:        0.01,
		CompactWindow:     30 * time.Second,
		CompactThreshold:  100,
		CompactMaxBuckets: 10000,
	}
}

// Validate validates shared log safety options.
func (c Config) Validate() error {
	if c.MaxRatePerSecond < 0 {
		return errors.Errorf("max_rate_per_second must be non-negative, got %d", c.MaxRatePerSecond)
	}

	switch c.Mode() {
	case ModeConsume, ModeDrop, ModeSample, ModeCompact, ModeTruncate:
	default:
		return errors.Errorf("on_excess must be one of consume, drop, sample, compact, truncate, got %q", c.OnExcess)
	}

	if c.SampleRate < 0 || c.SampleRate > 1 {
		return errors.Errorf("sample_rate must be in [0.0, 1.0], got %f", c.SampleRate)
	}

	if c.Mode() == ModeCompact || c.Mode() == ModeTruncate {
		if c.CompactWindow <= 0 {
			return errors.Errorf("compact_window must be positive, got %s", c.CompactWindow)
		}
	}

	if c.Mode() == ModeCompact {
		if c.CompactThreshold <= 0 {
			return errors.Errorf("compact_threshold must be positive, got %d", c.CompactThreshold)
		}
		if c.CompactMaxBuckets <= 0 {
			return errors.Errorf("compact_max_buckets must be positive, got %d", c.CompactMaxBuckets)
		}
		if c.TruncateThreshold < 0 {
			return errors.Errorf("truncate_threshold must be non-negative, got %d", c.TruncateThreshold)
		}
	}

	return nil
}

// Mode returns the configured excess handling mode with defaults applied.
func (c Config) Mode() string {
	if c.OnExcess == "" {
		return ModeConsume
	}
	return c.OnExcess
}
