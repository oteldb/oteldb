package storagebackend

import (
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
