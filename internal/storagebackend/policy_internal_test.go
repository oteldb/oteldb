package storagebackend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"

	"github.com/go-faster/yaml"
)

// resolvePolicy builds the tenancy option from cfg and returns the resolver the storage library
// would consult.
func resolvePolicy(t *testing.T, cfg *PolicyConfig) tenant.Resolver {
	t.Helper()

	opt, err := tenancyOption(cfg)
	require.NoError(t, err)
	require.NotNil(t, opt)

	var opts storage.Options
	opt(&opts)
	require.NotNil(t, opts.Tenancy)

	return opts.Tenancy
}

// TestPolicyPerTenant pins that a named tenant gets its own policy and every other tenant gets the
// default one declared inline.
func TestPolicyPerTenant(t *testing.T) {
	r := resolvePolicy(t, &PolicyConfig{
		PolicyRules: PolicyRules{
			Retention: &RetentionConfig{MaxAge: 24 * time.Hour},
		},
		Tenants: map[string]PolicyRules{
			"acme": {Retention: &RetentionConfig{MaxAge: 30 * 24 * time.Hour}},
		},
	})

	require.Equal(t, 30*24*time.Hour, r.Resolve(signal.TenantID("acme")).Retention.MaxAge)
	require.Equal(t, 24*time.Hour, r.Resolve(signal.TenantID("globex")).Retention.MaxAge,
		"an unlisted tenant falls back to the default policy")
	require.Equal(t, 24*time.Hour, r.Resolve(signal.TenantID("default")).Retention.MaxAge)
}

// TestPolicyPerTenantWithoutDefault pins that per-tenant policies alone install a resolver, and
// that an unlisted tenant then gets the zero policy rather than nothing being installed.
func TestPolicyPerTenantWithoutDefault(t *testing.T) {
	r := resolvePolicy(t, &PolicyConfig{
		Tenants: map[string]PolicyRules{
			"acme": {Retention: &RetentionConfig{MaxAge: time.Hour}},
		},
	})

	require.Equal(t, time.Hour, r.Resolve(signal.TenantID("acme")).Retention.MaxAge)
	require.Zero(t, r.Resolve(signal.TenantID("globex")).Retention.MaxAge)
}

// TestPolicyPerTenantValidation pins that a bad tenant id or a bad rule inside a tenant's block is a
// startup error rather than a policy that silently never applies.
func TestPolicyPerTenantValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  *PolicyConfig
	}{
		{
			name: "InvalidTenantID",
			cfg: &PolicyConfig{Tenants: map[string]PolicyRules{
				"acme/_s0": {Retention: &RetentionConfig{MaxAge: time.Hour}},
			}},
		},
		{
			name: "InvalidRule",
			cfg: &PolicyConfig{Tenants: map[string]PolicyRules{
				"acme": {Downsample: []DownsampleTierConfig{{After: time.Hour, Interval: time.Minute, Agg: "nope"}}},
			}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tenancyOption(tt.cfg)
			require.Error(t, err)
		})
	}
}

// TestPolicyYAMLShape pins that making the policy tenant-keyed did not move the default rules under
// a nested key: a config written before this change must still parse identically.
func TestPolicyYAMLShape(t *testing.T) {
	const src = `
retention:
  max_age: 24h
precision:
  - after: 1h
    bits: 10
tenants:
  acme:
    retention:
      max_age: 720h
`

	var cfg PolicyConfig
	require.NoError(t, yaml.Unmarshal([]byte(src), &cfg))

	require.NotNil(t, cfg.Retention)
	require.Equal(t, 24*time.Hour, cfg.Retention.MaxAge)
	require.Len(t, cfg.Precision, 1)
	require.Equal(t, uint8(10), cfg.Precision[0].Bits)

	acme, ok := cfg.Tenants["acme"]
	require.True(t, ok)
	require.NotNil(t, acme.Retention)
	require.Equal(t, 720*time.Hour, acme.Retention.MaxAge)
}
