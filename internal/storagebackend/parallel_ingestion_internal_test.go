package storagebackend

import (
	"encoding/json"
	"testing"

	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
)

func TestMetricAsyncPressureFlushConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cfg  Config
		err  string
	}{
		{name: "default"},
		{name: "file", cfg: Config{Backend: "file", MetricDiskSeries: true, MetricAsyncPressureFlush: true}},
		{name: "s3-wal", cfg: Config{Backend: "s3", WALDir: "wal", MetricDiskSeries: true, MetricAsyncPressureFlush: true}},
		{name: "no-index", cfg: Config{Backend: "file", MetricAsyncPressureFlush: true}, err: "requires metric_disk_series"},
		{name: "no-wal", cfg: Config{Backend: "s3", MetricDiskSeries: true, MetricAsyncPressureFlush: true}, err: "requires a WAL directory"},
		{
			name: "read-only", cfg: Config{Backend: "file", MetricDiskSeries: true, MetricAsyncPressureFlush: true, ReadOnly: true},
			err: "requires a standalone writable engine",
		},
		{name: "cluster", cfg: Config{
			Backend: "file", MetricDiskSeries: true, MetricAsyncPressureFlush: true,
			Cluster: &ClusterConfig{Etcd: []string{"localhost:2379"}},
		}, err: "requires a standalone writable engine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.validate()
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.cfg.MetricAsyncPressureFlush, storeOptions(tc.cfg, cacheSettings{}).MetricAsyncPressureFlush)
		})
	}
	const data = `{"backend":"file","metric_disk_series":true,"metric_async_pressure_flush":true}`
	var jsonConfig, yamlConfig Config
	require.NoError(t, json.Unmarshal([]byte(data), &jsonConfig))
	require.NoError(t, yaml.Unmarshal([]byte(data), &yamlConfig))
	require.True(t, jsonConfig.MetricAsyncPressureFlush)
	require.Equal(t, jsonConfig, yamlConfig)
}
