package storagebackend

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestTenantPolicyBlocks(t *testing.T) {
	t.Parallel()
	base := &PolicyConfig{
		Retention:  &RetentionConfig{MaxAge: time.Hour, MaxBytes: 1000, Signals: map[string]SignalRetentionConfig{"log": {MaxAge: 2 * time.Hour}}},
		Limits:     &LimitsConfig{MaxSeries: 100},
		Precision:  []PrecisionTierConfig{{After: time.Minute, Bits: 32}},
		Downsample: []DownsampleTierConfig{{After: time.Minute, Interval: time.Second}},
		Recompress: &RecompressConfig{After: time.Minute, Level: 9},
		EC:         &ECConfig{Data: 4, Parity: 2},
	}
	for _, tc := range []struct {
		name     string
		override *PolicyConfig
	}{
		{name: "nil inherits"},
		{name: "empty inherits", override: &PolicyConfig{}},
		{name: "retention", override: &PolicyConfig{Retention: &RetentionConfig{MaxAge: 3 * time.Hour}}},
		{name: "limits", override: &PolicyConfig{Limits: &LimitsConfig{MaxSeries: 10}}},
		{name: "precision", override: &PolicyConfig{Precision: []PrecisionTierConfig{{Bits: 16}}}},
		{name: "downsample", override: &PolicyConfig{Downsample: []DownsampleTierConfig{{Interval: time.Minute, Agg: "sum"}}}},
		{name: "recompress", override: &PolicyConfig{Recompress: &RecompressConfig{After: time.Hour, Level: 3}}},
		{name: "ec", override: &PolicyConfig{EC: &ECConfig{Data: 2, Parity: 1}}},
		{name: "empty blocks replace", override: &PolicyConfig{Retention: &RetentionConfig{}, Limits: &LimitsConfig{}, Precision: []PrecisionTierConfig{}, Downsample: []DownsampleTierConfig{}, Recompress: &RecompressConfig{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resolver, err := tenancyResolver(base, map[string]*PolicyConfig{"team-a": tc.override})
			require.NoError(t, err)
			fallback, err := base.policy()
			require.NoError(t, err)
			require.Equal(t, fallback, resolver.Resolve("other"))
			expected := fallback
			if o := tc.override; o != nil {
				p, err := o.policy()
				require.NoError(t, err)
				if o.Retention != nil {
					expected.Retention = p.Retention
				}
				if o.Limits != nil {
					expected.Limits = p.Limits
				}
				if o.Precision != nil {
					expected.Precision = p.Precision
				}
				if o.Downsample != nil {
					expected.Downsample = p.Downsample
				}
				if o.Recompress != nil {
					expected.Recompress = p.Recompress
				}
				if o.EC != nil {
					expected.Durability = p.Durability
				}
			}
			require.Equal(t, expected, resolver.Resolve("team-a"))
		})
	}
}

func TestTenantPolicyValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		id      string
		policy  *PolicyConfig
		message string
	}{
		{name: "empty tenant", message: `tenant ""`},
		{name: "negative retention", id: "team-a", policy: &PolicyConfig{Retention: &RetentionConfig{MaxAge: -time.Hour}}, message: "retention"},
		{name: "unknown signal", id: "team-a", policy: &PolicyConfig{Retention: &RetentionConfig{Signals: map[string]SignalRetentionConfig{"logs": {}}}}, message: "retention"},
		{name: "signal exceeds pool", id: "team-a", policy: &PolicyConfig{Retention: &RetentionConfig{MaxBytes: 10, Signals: map[string]SignalRetentionConfig{"log": {MaxBytes: 11}}}}, message: "retention"},
		{name: "limits", id: "team-a", policy: &PolicyConfig{Limits: &LimitsConfig{MaxSeries: 1, MaxSeriesSoft: 2}}, message: "limits"},
		{name: "ec", id: "team-a", policy: &PolicyConfig{EC: &ECConfig{Data: 1}}, message: "ec"},
		{name: "aggregation", id: "team-a", policy: &PolicyConfig{Downsample: []DownsampleTierConfig{{Agg: "oops"}}}, message: "downsample"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := Open(t.Context(), Config{Tenants: map[string]*PolicyConfig{tc.id: tc.policy}}, zap.NewNop(), &app.Telemetry{})
			require.ErrorContains(t, err, tc.message)
			require.ErrorContains(t, err, `tenant "`+tc.id+`"`)
		})
	}
}

func TestTenantPolicyConfig(t *testing.T) {
	t.Parallel()
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`policy:
  precision: [{after: 1h, bits: 32}]
  downsample: [{after: 1h, interval: 1m}]
  retention: {max_age: 24h}
tenants:
  team-a:
    precision: []
    downsample: []
    retention: {}
  team-b: null
`), &cfg))
	resolver, err := tenancyResolver(cfg.Policy, cfg.Tenants)
	require.NoError(t, err)
	require.Empty(t, resolver.Resolve("team-a").Precision.Tiers)
	require.Empty(t, resolver.Resolve("team-a").Downsample.Tiers)
	require.Zero(t, resolver.Resolve("team-a").Retention.MaxAge)
	require.Equal(t, 24*time.Hour, resolver.Resolve("team-b").Retention.MaxAge)
	for _, value := range []string{"null", "{}", "{log: 1GB}"} {
		var invalid Config
		require.ErrorContains(t, yaml.Unmarshal([]byte("tenants:\n  team-a:\n    retention:\n      max_bytes_per_signal: "+value), &invalid), "was removed")
	}
}

func TestTenantPolicyMetadata(t *testing.T) {
	t.Parallel()
	cfg := Config{Tenants: map[string]*PolicyConfig{
		"default": {Retention: &RetentionConfig{MaxAge: time.Hour}},
		"team-a":  {Retention: &RetentionConfig{MaxAge: 2 * time.Hour, Signals: map[string]SignalRetentionConfig{"log": {MaxAge: 3 * time.Hour}}}},
	}}
	core, logs := observer.New(zap.InfoLevel)
	b, stop, err := Open(t.Context(), cfg, zap.New(core), &app.Telemetry{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
	for id, age := range map[signal.TenantID]time.Duration{"": time.Hour, "default": time.Hour, "team-a": 3 * time.Hour, "team-a/_s3": 3 * time.Hour, "other": 0} {
		policy, known := b.RetentionPolicy(id)
		require.True(t, known)
		require.Equal(t, age, policy.AgeFor(signal.Log))
	}
	cfg.Tenants["team-a"].Retention.Signals["log"] = SignalRetentionConfig{MaxAge: time.Nanosecond}
	cfg.Tenants["default"].Retention.MaxAge = time.Nanosecond
	delete(cfg.Tenants, "team-a")
	policy, _ := b.RetentionPolicy("team-a")
	policy.PerSignal[signal.Log] = tenant.SignalRetention{MaxAge: time.Nanosecond}
	again, _ := b.RetentionPolicy("team-a")
	require.Equal(t, 3*time.Hour, again.AgeFor(signal.Log))
	fallback, _ := b.RetentionPolicy("default")
	require.Equal(t, time.Hour, fallback.MaxAge)
	for _, id := range []string{"*", "default", "team-a"} {
		require.Equal(t, 5, logs.FilterMessage("Effective embedded storage retention").FilterField(zap.String("tenant", id)).Len())
	}
}

func TestTenantPolicyEffectiveWarnings(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Policy: &PolicyConfig{
			Retention: &RetentionConfig{MaxAge: 24 * time.Hour},
			Precision: []PrecisionTierConfig{{After: 2 * time.Hour, Bits: 32}},
			EC:        &ECConfig{Data: 4, Parity: 2},
		},
		Tenants: map[string]*PolicyConfig{
			"short": {Retention: &RetentionConfig{MaxAge: time.Hour}},
			"long":  {Retention: &RetentionConfig{MaxAge: 72 * time.Hour}},
		},
	}
	core, logs := observer.New(zap.WarnLevel)
	b, stop, err := Open(t.Context(), cfg, zap.New(core), &app.Telemetry{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
	policy, known := b.RetentionPolicy("short")
	require.True(t, known)
	require.Equal(t, time.Hour, policy.MaxAge)
	tierWarnings := logs.FilterMessage("Storage policy tier starts at or after metric retention expires")
	require.Equal(t, 1, tierWarnings.Len())
	require.Equal(t, "short", tierWarnings.All()[0].ContextMap()["tenant"])
	for _, id := range []string{"*", "short", "long"} {
		require.Equal(t, 1, logs.FilterMessage("storage policy sets erasure coding, but it cannot apply").FilterField(zap.String("tenant", id)).Len())
	}
}
