package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	data := `
prometheus:
  disable_rate_offloading: true
  disable_metric_offloading: true
  cache:
    max_bytes: 100MiB
max_result_bytes: 1GB
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	assert.Equal(t, int64(100*1024*1024), int64(cfg.Prometheus.Cache.MaxBytes))
	assert.Equal(t, int64(1000*1000*1000), int64(cfg.MaxResultBytes))
	assert.True(t, cfg.Prometheus.DisableRateOffloading)
	assert.True(t, cfg.Prometheus.DisableMetricOffloading)
}

// TestLoadConfigRejectsOversizedBytes pins that a size too large for [xbytes.Bytes] fails the load
// instead of wrapping negative, for every layer a size reaches: the binary's own config, a shared
// block, and the embedded storage policy. A negative reads as "off" downstream, so the operator
// would otherwise get no bound at all.
func TestLoadConfigRejectsOversizedBytes(t *testing.T) {
	for name, data := range map[string]string{
		"MaxResultBytes":    "max_result_bytes: 10EB\n",
		"MetricsCache":      "prometheus:\n  cache:\n    max_bytes: 10EB\n",
		"RetentionMaxBytes": "storage:\n  policy:\n    retention:\n      max_bytes: 10EB\n",
	} {
		t.Run(name, func(t *testing.T) {
			f, err := os.CreateTemp("", "oteldb.yml")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			_, err = f.WriteString(data)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			_, err = loadConfig(f.Name())
			require.Error(t, err)
			require.Contains(t, err.Error(), "10EB")
		})
	}
}

// TestLoadConfigListeners pins the per-signal bind/auth shape, which the blocks now inherit from
// an embedded [config.Listener]: the keys must stay where existing config files put them.
func TestLoadConfigListeners(t *testing.T) {
	data := `
prometheus:
  bind: ":19090"
  auth:
    - type: bearertoken
      tokens:
        - token: secret
loki:
  bind: ":13100"
  drilldown_enabled: true
tempo:
  bind: ":13200"
pyroscope:
  bind: ":14040"
admin:
  bind: ":18090"
health_check:
  bind: ":23133"
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	assert.Equal(t, ":19090", cfg.Prometheus.Bind)
	require.Len(t, cfg.Prometheus.Auth, 1)
	assert.Equal(t, AuthTypeBearerToken, cfg.Prometheus.Auth[0].Type)
	assert.Equal(t, ":13100", cfg.Loki.Bind)
	assert.True(t, cfg.Loki.DrilldownEnabled)
	assert.Equal(t, ":13200", cfg.Tempo.Bind)
	assert.Equal(t, ":14040", cfg.Pyroscope.Bind)
	assert.Equal(t, ":18090", cfg.Admin.Bind)
	assert.Equal(t, ":23133", cfg.HealthCheck.Bind)
}
