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

	// PassthroughAttribute is the record attribute key that, when set to a
	// true boolean value, bypasses excess handling entirely for that record.
	PassthroughAttribute = "oteldb.passthrough" //#nosec G101 -- not a credential, an attribute key
)

// Config defines shared log safety options.
type Config struct {
	// SoftMaxRatePerSecond is the rate above which Mode's excess handling
	// engages. See SoftLimit.
	SoftMaxRatePerSecond int `mapstructure:"soft_max_rate_per_second"`
	// HardMaxRatePerSecond is the rate above which HardMode's excess handling
	// engages, escalating past the soft limit. See HardLimit.
	HardMaxRatePerSecond int `mapstructure:"hard_max_rate_per_second"`

	// OnExcess is the excess handling mode applied once the soft limit is
	// exceeded. See Mode.
	OnExcess string `mapstructure:"on_excess"`
	// HardOnExcess is the excess handling mode applied once the hard limit is
	// exceeded, overriding OnExcess. See HardMode.
	HardOnExcess string `mapstructure:"hard_on_excess"`

	// SampleFirst is the number of occurrences logged unconditionally before
	// SampleThereafter-based sampling kicks in, for ModeSample.
	SampleFirst int `mapstructure:"sample_first"`
	// SampleThereafter samples 1-in-N occurrences once SampleFirst has been
	// exceeded, for ModeSample.
	SampleThereafter int `mapstructure:"sample_thereafter"`

	// CompactWindow is the duration each compaction/truncation bucket covers
	// before it's flushed, for ModeCompact and ModeTruncate.
	CompactWindow time.Duration `mapstructure:"compact_window"`
	// CompactThreshold is the minimum number of records collapsed into a
	// bucket before it emits a synthetic "N collapsed" record, for ModeCompact.
	CompactThreshold int `mapstructure:"compact_threshold"`
	// CompactMaxBuckets caps the number of concurrently tracked compaction
	// buckets, bounding memory use, for ModeCompact.
	CompactMaxBuckets int `mapstructure:"compact_max_buckets"`
	// CompactKeyFields are the record fields used to group records into the
	// same compaction bucket. If empty, records are keyed by body alone.
	CompactKeyFields []string `mapstructure:"compact_key_fields"`
	// TruncateThreshold is the minimum number of records suppressed in a
	// window before a synthetic "N suppressed" record is emitted, for
	// ModeTruncate.
	TruncateThreshold int `mapstructure:"truncate_threshold"`

	// RedactFields are record attribute keys whose values are replaced with
	// "<redacted>" before excess handling runs.
	RedactFields []string `mapstructure:"redact_fields"`
}

// DefaultConfig returns shared log safety defaults.
func DefaultConfig() Config {
	return Config{
		OnExcess:          ModeConsume,
		HardOnExcess:      ModeDrop,
		SampleFirst:       100,
		SampleThereafter:  100,
		CompactWindow:     30 * time.Second,
		CompactThreshold:  100,
		CompactMaxBuckets: 10000,
	}
}

// Validate validates shared log safety options.
func (c Config) Validate() error {
	if c.SoftMaxRatePerSecond < 0 {
		return errors.Errorf("soft_max_rate_per_second must be non-negative, got %d", c.SoftMaxRatePerSecond)
	}
	if c.HardMaxRatePerSecond < 0 {
		return errors.Errorf("hard_max_rate_per_second must be non-negative, got %d", c.HardMaxRatePerSecond)
	}

	validateMode := func(mode string, field string) error {
		switch mode {
		case ModeConsume, ModeDrop, ModeSample, ModeCompact, ModeTruncate, "":
			return nil
		default:
			return errors.Errorf("%s must be one of consume, drop, sample, compact, truncate, got %q", field, mode)
		}
	}
	if err := validateMode(c.OnExcess, "on_excess"); err != nil {
		return err
	}
	if err := validateMode(c.HardOnExcess, "hard_on_excess"); err != nil {
		return err
	}

	if c.SampleFirst < 0 {
		return errors.Errorf("sample_first must be non-negative, got %d", c.SampleFirst)
	}
	if c.SampleThereafter < 0 {
		return errors.Errorf("sample_thereafter must be non-negative, got %d", c.SampleThereafter)
	}

	mode := c.Mode()
	hardMode := c.HardMode()
	if mode == ModeCompact || mode == ModeTruncate || hardMode == ModeCompact || hardMode == ModeTruncate {
		if c.CompactWindow <= 0 {
			return errors.Errorf("compact_window must be positive, got %s", c.CompactWindow)
		}
	}

	if mode == ModeCompact || hardMode == ModeCompact {
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

// HardMode returns the configured hard excess handling mode with defaults applied.
func (c Config) HardMode() string {
	if c.HardOnExcess == "" {
		return ModeDrop
	}
	return c.HardOnExcess
}

// SoftLimit returns the effective soft rate limit.
func (c Config) SoftLimit() int {
	return c.SoftMaxRatePerSecond
}

// HardLimit returns the effective hard rate limit.
func (c Config) HardLimit() int {
	return c.HardMaxRatePerSecond
}
