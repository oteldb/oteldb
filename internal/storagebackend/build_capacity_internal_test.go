package storagebackend

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/xbytes"
)

func TestBuildCapacityOptions(t *testing.T) {
	t.Parallel()
	build, flush := xbytes.Bytes(2048<<20), xbytes.Bytes(448<<20)
	cfg := Config{BuildMemoryBytes: &build, MetricFlushMemoryBytes: &flush}
	require.NoError(t, cfg.validate())
	opts := storeOptions(cfg, resolveCacheSettings(cfg))
	require.EqualValues(t, build, opts.BuildMemoryBytes)
	require.EqualValues(t, flush, opts.MetricFlushMemoryBytes)
	require.Zero(t, opts.MergeMemoryBytes)
	negative := xbytes.Bytes(-1)
	for _, c := range []Config{{BuildMemoryBytes: &negative}, {MetricFlushMemoryBytes: &negative}} {
		require.Error(t, c.validate())
	}
}

func TestPressureGenerationOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		generations  int
		async, valid bool
	}{
		{name: "default", valid: true},
		{name: "one", generations: 1, valid: true},
		{name: "two", generations: 2, async: true, valid: true},
		{name: "requires pipeline", generations: 2},
		{name: "negative", generations: -1},
		{name: "too many", generations: 3, async: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Backend: "file", MetricDiskSeries: true,
				MetricAsyncPressureFlush: tc.async, MetricPressureGenerations: tc.generations,
			}
			if !tc.valid {
				require.Error(t, cfg.validate())
				return
			}
			require.NoError(t, cfg.validate())
			require.Equal(t, tc.generations, storeOptions(cfg, cacheSettings{}).MetricPressureGenerations)
		})
	}
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"backend":"file","metric_disk_series":true,"metric_async_pressure_flush":true,"metric_pressure_generations":2}`), &cfg))
	require.NoError(t, cfg.validate())
	require.Equal(t, 2, cfg.MetricPressureGenerations)
}

func TestSeparateSeriesOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		disk, chunks bool
	}{
		{name: "supported", disk: true, chunks: true},
		{name: "requires disk index", chunks: true},
		{name: "requires chunks", disk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Backend: "file", MetricSeparateSeries: true,
				MetricDiskSeries: tc.disk, MetricSeriesChunks: tc.chunks,
			}
			if !tc.disk || !tc.chunks {
				require.ErrorContains(t, cfg.validate(), "metric_separate_series requires")
				return
			}
			require.NoError(t, cfg.validate())
			require.True(t, storeOptions(cfg, cacheSettings{}).MetricSeparateSeries)
		})
	}
}
