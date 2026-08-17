package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

type testConfig struct {
	Prometheus config.Prometheus `json:"prometheus" yaml:"prometheus"`
	Cluster    config.Cluster    `json:"cluster" yaml:"cluster"`
}

func TestLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
prometheus:
  bind: ":9091"
  max_samples: 10
cluster:
  etcd: ["127.0.0.1:2379"]
  shards_per_tenant: 8
`), 0o600))

	cfg, err := config.Load[testConfig](path, config.LoadOptions{})
	require.NoError(t, err)
	assert.Equal(t, ":9091", cfg.Prometheus.Bind)
	assert.Equal(t, 10, cfg.Prometheus.MaxSamples)
	assert.Equal(t, []string{"127.0.0.1:2379"}, cfg.Cluster.Etcd)
	assert.Equal(t, 8, cfg.Cluster.ShardsPerTenant)
}

func TestLoadFallback(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing.yml")

	t.Run("Optional", func(t *testing.T) {
		t.Parallel()

		cfg, err := config.Load[testConfig]("", config.LoadOptions{Fallback: missing, Optional: true})
		require.NoError(t, err)
		assert.Zero(t, cfg)
	})
	t.Run("Required", func(t *testing.T) {
		t.Parallel()

		_, err := config.Load[testConfig]("", config.LoadOptions{Fallback: missing})
		require.Error(t, err)
	})
	t.Run("ExplicitPathIsRequired", func(t *testing.T) {
		t.Parallel()

		_, err := config.Load[testConfig](missing, config.LoadOptions{Fallback: missing, Optional: true})
		require.Error(t, err)
	})
}

func TestSetDefaults(t *testing.T) {
	t.Parallel()

	var prom config.Prometheus
	prom.SetDefaults()
	assert.Equal(t, ":9090", prom.Bind)
	assert.Equal(t, 50_000_000, prom.MaxSamples)
	assert.Equal(t, 1_000_000, prom.MaxTimeseries)
	assert.Equal(t, time.Minute, prom.Timeout)
	require.NotNil(t, prom.EnableNegativeOffset)
	assert.True(t, *prom.EnableNegativeOffset)

	var loki config.Loki
	loki.SetDefaults()
	assert.Equal(t, ":3100", loki.Bind)
	assert.Equal(t, 1_000_000, loki.MaxSampleRows)
	assert.Equal(t, int64(256*1024*1024), int64(loki.MaxSampleResultBytes))

	var tempo config.Tempo
	tempo.SetDefaults()
	assert.Equal(t, ":3200", tempo.Bind)

	var pyro config.Pyroscope
	pyro.SetDefaults()
	assert.Equal(t, ":4040", pyro.Bind)

	var admin config.Admin
	admin.SetDefaults()
	assert.Equal(t, ":8090", admin.Bind)

	var health config.HealthCheck
	health.SetDefaults()
	assert.Equal(t, ":13133", health.Bind)

	var auth config.Auth
	auth.SetDefaults()
	assert.Equal(t, config.AuthTypeNone, auth.Type)
}

func TestAuthMiddleware(t *testing.T) {
	t.Parallel()

	m, err := config.AuthMiddleware(nil)
	require.NoError(t, err)
	assert.Nil(t, m)

	m, err = config.AuthMiddleware([]config.Auth{{
		Type:   config.AuthTypeBearerToken,
		Tokens: []httpmiddleware.Token{{Token: "secret"}},
	}})
	require.NoError(t, err)
	require.NotNil(t, m)

	_, err = config.AuthMiddleware([]config.Auth{{Type: "unknown"}})
	require.Error(t, err)
}
