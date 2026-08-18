package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// TestValidateTenancy pins that tenancy can only be enabled where it is actually enforced: every
// queryable signal must be served by the embedded storage engine, since a ClickHouse-backed signal
// has no tenant scoping and would answer every credential from the same tables.
func TestValidateTenancy(t *testing.T) {
	enabled := config.Tenancy{
		Enabled: true,
		Tokens: []config.TenancyToken{{
			Token:   httpmiddleware.Token{Token: "tok"},
			Tenants: []string{"acme"},
		}},
	}

	t.Run("DisabledIsAlwaysValid", func(t *testing.T) {
		cfg := Config{}
		cfg.setDefaults()
		require.NoError(t, cfg.validateTenancy())
	})

	t.Run("ClickHouseSignalRefused", func(t *testing.T) {
		cfg := Config{Tenancy: enabled}
		cfg.setDefaults()
		require.Error(t, cfg.validateTenancy())
	})

	t.Run("PartialStorageRefused", func(t *testing.T) {
		cfg := Config{Tenancy: enabled}
		cfg.useEmbeddedStorage()
		cfg.LogsBackend = MetricsBackendClickHouse
		cfg.setDefaults()
		require.Error(t, cfg.validateTenancy())
	})

	t.Run("FullyEmbeddedAccepted", func(t *testing.T) {
		cfg := Config{Tenancy: enabled}
		cfg.useEmbeddedStorage()
		cfg.setDefaults()
		require.NoError(t, cfg.validateTenancy())
	})
}

// TestTenancyConfigYAML checks the tenancy block parses from the documented YAML shape via the real
// config loader.
func TestTenancyConfigYAML(t *testing.T) {
	const data = `
tenancy:
  enabled: true
  selector_header: X-Scope-OrgID
  tokens:
    - token: acme-secret
      tenants: [acme, acme-staging]
      username: alice
`

	f, err := os.CreateTemp(t.TempDir(), "oteldb.yml")
	require.NoError(t, err)
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.True(t, cfg.Tenancy.Enabled)
	require.Equal(t, "X-Scope-OrgID", cfg.Tenancy.SelectorHeader)
	require.Len(t, cfg.Tenancy.Tokens, 1)
	require.Equal(t, "acme-secret", cfg.Tenancy.Tokens[0].Token.Token)
	require.Equal(t, []string{"acme", "acme-staging"}, cfg.Tenancy.Tokens[0].Tenants)
	require.Equal(t, "alice", cfg.Tenancy.Tokens[0].Username)

	m, err := config.TenancyMiddleware(cfg.Tenancy)
	require.NoError(t, err)
	require.NotNil(t, m)
}
