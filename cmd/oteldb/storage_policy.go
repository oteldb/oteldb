package main

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// StoragePolicyConfig configures the per-tenant storage policy applied to the embedded engine's
// background merges: age-tiered lossy float precision, downsampling, and cold-data recompression.
// The storage library resolves these per-tenant via a [tenant.Resolver] callback; oteldb runs the
// embedded engine single-tenant (every signal routes to the "default" tenant), so this one policy
// is resolved for every tenant. Empty ⇒ no tenancy resolver is installed (the library default:
// lossless, no rollup, no recompression).
type StoragePolicyConfig struct {
	// Precision is the age-tiered lossy float-compression policy: each tier re-encodes, at merge,
	// the value column of parts older than After to retain only Bits significant mantissa bits, so
	// recent data stays lossless and only old data trades accuracy for size. Empty ⇒ lossless.
	Precision []PrecisionTierConfig `json:"precision" yaml:"precision"`
	// Downsample is the age-tiered merge-time rollup: each tier replaces samples older than After
	// with one representative per Interval-wide bucket, the bucket combined by Agg. Empty ⇒ raw.
	Downsample []DownsampleTierConfig `json:"downsample" yaml:"downsample"`
	// Recompress rewrites fully-cold parts (older than After) with a higher-ratio Zstandard profile
	// at merge, trading merge CPU for storage. It is decode-transparent. Nil ⇒ disabled.
	Recompress *RecompressConfig `json:"recompress" yaml:"recompress"`
}

// PrecisionTierConfig is one age band of the lossy float-precision policy.
type PrecisionTierConfig struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration `json:"after" yaml:"after"`
	// Bits is the significant mantissa bits retained (1..63). 0 or ≥64 ⇒ lossless (ignored).
	Bits uint8 `json:"bits" yaml:"bits"`
}

// DownsampleTierConfig is one age band of the downsampling policy.
type DownsampleTierConfig struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration `json:"after" yaml:"after"`
	// Interval is the rollup bucket width. Zero ⇒ the tier is disabled.
	Interval time.Duration `json:"interval" yaml:"interval"`
	// Agg combines a bucket's samples: "last" (default), "first", "min", "max", "sum", "avg",
	// "count". Empty ⇒ "last".
	Agg string `json:"agg" yaml:"agg"`
}

// RecompressConfig configures cold-data recompression.
type RecompressConfig struct {
	// After is the age past which a fully-cold part is recompressed at merge. Zero is invalid here
	// (a Recompress block is present only to enable it); use a positive age.
	After time.Duration `json:"after" yaml:"after"`
	// Level is the Zstandard level (1 fastest … 19 best ratio). Zero ⇒ the best-ratio default.
	Level int `json:"level" yaml:"level"`
}

// empty reports whether the policy configures nothing, in which case no resolver is installed.
func (cfg *StoragePolicyConfig) empty() bool {
	return cfg == nil || (len(cfg.Precision) == 0 && len(cfg.Downsample) == 0 && cfg.Recompress == nil)
}

// tenancyOption builds the storage tenancy option from the policy config, or returns (nil, nil)
// when no policy is configured. The resolved policy is applied to every tenant — oteldb runs the
// embedded engine single-tenant, so a static resolver suffices.
func tenancyOption(cfg *StoragePolicyConfig) (storage.Option, error) {
	if cfg.empty() {
		return nil, nil
	}

	policy, err := cfg.policy()
	if err != nil {
		return nil, err
	}

	return storage.WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return policy
	})), nil
}

// policy translates the config into a [tenant.Policy]. It validates the downsample aggregation
// names so a typo is a startup error rather than a silently-ignored tier.
func (cfg *StoragePolicyConfig) policy() (tenant.Policy, error) {
	var p tenant.Policy

	for _, t := range cfg.Precision {
		p.Precision.Tiers = append(p.Precision.Tiers, tenant.PrecisionTier{
			After: t.After,
			Bits:  t.Bits,
		})
	}

	for i, t := range cfg.Downsample {
		agg := signal.AggLast
		if t.Agg != "" {
			parsed, err := signal.ParseAggregation(t.Agg)
			if err != nil {
				return tenant.Policy{}, errors.Wrapf(err, "downsample tier %d", i)
			}
			agg = parsed
		}
		p.Downsample.Tiers = append(p.Downsample.Tiers, tenant.DownsampleTier{
			After:    t.After,
			Interval: t.Interval,
			Agg:      agg,
		})
	}

	if r := cfg.Recompress; r != nil {
		p.Recompress = tenant.Recompress{After: r.After, Level: r.Level}
	}

	return p, nil
}
