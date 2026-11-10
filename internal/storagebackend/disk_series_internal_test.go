package storagebackend

import (
	"encoding/json"
	"testing"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/xbytes"
)

func TestMetricDiskSeriesConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		enabled    bool
		bytes      int64
		cacheBytes int64
		err        string
		parseError bool
	}{
		{name: "defaults", data: "{}"},
		{name: "enabled", data: "backend: file\nmetric_disk_series: true", enabled: true},
		{name: "bounded", data: "backend: file\nmetric_disk_series: true\nmetric_resident_head_bytes: 16 MiB", enabled: true, bytes: 16 << 20},
		{name: "disabled", data: "metric_disk_series: false\nmetric_resident_head_bytes: 8 MiB", bytes: 8 << 20},
		{name: "zero", data: "metric_resident_head_bytes: 0", err: "must be positive"},
		{name: "negative", data: "metric_resident_head_bytes: -1 MiB", parseError: true},
		{name: "cache", data: "backend: file\nmetric_disk_series: true\nmetric_series_cache_bytes: 64 MiB", enabled: true, cacheBytes: 64 << 20},
		{name: "cache-disabled-mode", data: "metric_series_cache_bytes: 8 MiB", cacheBytes: 8 << 20},
		{name: "cache-zero", data: "metric_series_cache_bytes: 0", err: "metric_series_cache_bytes: must be positive"},
		{name: "cache-negative", data: "metric_series_cache_bytes: -1 MiB", parseError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg Config
			err := yaml.Unmarshal([]byte(tc.data), &cfg)
			if tc.parseError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			cfg.SetDefaults()
			err = cfg.validate()
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			opts := storeOptions(cfg, cacheSettings{MaxQuery: 123})
			require.Equal(t, tc.enabled, opts.MetricDiskSeries)
			require.Equal(t, tc.bytes, opts.MetricResidentHeadBytes)
			require.Equal(t, tc.cacheBytes, opts.MetricSeriesCacheBytes)
			require.EqualValues(t, 123, opts.MaxQueryBytes)
		})
	}
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"backend":"file","metric_disk_series":true,"metric_resident_head_bytes":"32 MiB","metric_series_cache_bytes":"64 MiB"}`), &cfg))
	require.NoError(t, cfg.validate())
	require.True(t, cfg.MetricDiskSeries)
	require.EqualValues(t, 32<<20, *cfg.MetricResidentHeadBytes)
	require.EqualValues(t, 64<<20, *cfg.MetricSeriesCacheBytes)
}

func TestMetricResidentHeadValidation(t *testing.T) {
	t.Parallel()
	for _, value := range []xbytes.Bytes{-1, 0} {
		cfg := Config{MetricResidentHeadBytes: new(value)}
		err := cfg.validate()
		require.ErrorContains(t, err, "metric_resident_head_bytes")
	}
}

func TestMetricSeriesCacheValidation(t *testing.T) {
	t.Parallel()
	for _, value := range []xbytes.Bytes{-1, 0} {
		cfg := Config{MetricSeriesCacheBytes: new(value)}
		err := cfg.validate()
		require.ErrorContains(t, err, "metric_series_cache_bytes")
	}
}

func TestMetricDiskSeriesRejectsMemoryBackend(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"", "memory"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Backend: kind, MetricDiskSeries: true}
			_, _, err := Open(t.Context(), cfg, zap.NewNop(), &app.Telemetry{})
			require.ErrorContains(t, err, "metric_disk_series requires a file or s3 backend")
		})
	}
	for _, kind := range []string{"file", "s3"} {
		require.NoError(t, (&Config{Backend: kind, MetricDiskSeries: true}).validate())
	}
}
