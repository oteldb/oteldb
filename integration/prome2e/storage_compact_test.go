package prome2e_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageFileIdentityRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	be, err := file.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	options := storage.Options{
		Backend: be, WALDir: filepath.Join(dir, "wal"), FlushInterval: int64(time.Hour),
		ReadCacheBytes: 2 << 20, DecodeCacheBytes: 4 << 20,
	}
	store, err := storage.Open(ctx, options)
	require.NoError(t, err)
	firstStore := store
	t.Cleanup(func() { require.NoError(t, firstStore.Close(context.Background())) })
	base := time.Unix(2_000_000, 0)
	for round := range 2 {
		metrics := pmetric.NewMetrics()
		scope := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		scope.Scope().SetName("node-exporter")
		metric := scope.Metrics().AppendEmpty()
		metric.SetName("node_cpu_seconds_total")
		cpu := metric.SetEmptySum()
		cpu.SetIsMonotonic(true)
		cpu.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for host := range 260 {
			for core := range 8 {
				for _, mode := range []string{"user", "system"} {
					point := cpu.DataPoints().AppendEmpty()
					point.Attributes().PutStr("instance", "host-"+strconv.Itoa(host))
					point.Attributes().PutStr("job", "node_exporter")
					point.Attributes().PutStr("cpu", strconv.Itoa(core))
					point.Attributes().PutStr("mode", mode)
					point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(round) * 15 * time.Second)))
					value := float64(host + core + round)
					if mode == "user" {
						value += float64(round)
					}
					point.SetDoubleValue(value)
				}
			}
		}
		require.NoError(t, storagebackend.New(store).ConsumeMetrics(ctx, metrics))
		require.NoError(t, store.Admin().Flush(ctx, "", signal.Metric))
	}
	require.NoError(t, store.Close(ctx))
	store, err = storage.Open(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
	end := pcommon.NewTimestampFromTime(base.Add(15 * time.Second))
	for _, tt := range []struct {
		name  string
		query string
		want  float64
	}{
		{name: "all", query: `count(node_cpu_seconds_total)`, want: 4160},
		{name: "host", query: `count(node_cpu_seconds_total{instance="host-17"})`, want: 16},
		{name: "negative", query: `count(node_cpu_seconds_total{instance="host-17",mode!="user"})`, want: 8},
		{name: "rate", query: `sum(irate(node_cpu_seconds_total{instance="host-17",mode="user"}[1m]))`, want: 16.0 / 15},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			response, err := client.GetQuery(ctx, promapi.GetQueryParams{
				Query: tt.query, Time: promapi.NewOptPrometheusTimestamp(getPromTS(end)),
			})
			require.NoError(t, err)
			require.Len(t, response.Data.Vector.Result, 1)
			value, ok := response.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
			require.True(t, ok)
			require.InDelta(t, tt.want, float64(value), 1e-12)
		})
	}
	values, err := client.GetLabelValues(ctx, promapi.GetLabelValuesParams{
		Label: "mode", Match: []string{`node_cpu_seconds_total{instance="host-17"}`},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"system", "user"}, []string(values.Data))
}
