package prome2e_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/yaml"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestNativeDiskSeriesFileHTTPReopen(t *testing.T) {
	t.Parallel()
	for _, chunks := range []bool{false, true} {
		t.Run("chunks="+strconv.FormatBool(chunks), func(t *testing.T) {
			t.Parallel()
			testNativeDiskSeriesFileHTTPReopen(t, chunks)
		})
	}
}

func testNativeDiskSeriesFileHTTPReopen(t *testing.T, chunks bool) {
	t.Helper()
	ctx := t.Context()
	var cfg storagebackend.Config
	require.NoError(t, yaml.Unmarshal([]byte("backend: file\nmetric_disk_series: true\nmetric_resident_head_bytes: 16 MiB\nread_cache_bytes: 0\nnative_query_cache_bytes: 0\n"), &cfg))
	cfg.MetricSeriesChunks = chunks
	cfg.Dir = t.TempDir()
	cfg.FlushInterval = time.Hour
	open := func() (*storagebackend.Backend, func(context.Context) error) {
		backend, closeStore, err := storagebackend.Open(ctx, cfg, zap.NewNop(), &app.Telemetry{})
		require.NoError(t, err)
		return backend, closeStore
	}
	backend, closeStore := open()
	t.Cleanup(func() { require.NoError(t, closeStore(context.WithoutCancel(ctx))) })
	const base = int64(1700000000)
	write := func(round int) {
		t.Helper()
		request := sparsePagedWriteRequest(base + int64(round*75))
		for i := range request.Timeseries {
			series := &request.Timeseries[i]
			slope := 1.0
			for _, label := range series.Labels {
				if label.Name == "mode" && label.Value == "system" {
					slope = 2
				}
			}
			for j := range series.Samples {
				series.Samples[j].Value += slope * float64(round*75)
			}
		}
		raw, err := request.Marshal()
		require.NoError(t, err)
		handler := promrw.NewHandler(backend, promrw.HandlerConfig{Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour}})
		requestHTTP := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
		requestHTTP.Header.Set("Content-Type", "application/x-protobuf")
		requestHTTP.Header.Set("Content-Encoding", "snappy")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, requestHTTP)
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	}
	check := func(t *testing.T, end int64) {
		t.Helper()
		_, client := setupDB(t, integration.TraceProvider(t), backend)
		stamp := func(ts int64) promapi.PrometheusTimestamp {
			return promapi.PrometheusTimestamp(strconv.FormatInt(ts, 10))
		}
		result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
			Query: `sum by(instance)(irate(sparse_paged_cpu_seconds_total{instance="host-0",job="node_exporter",mode="system"}[1m]))`,
			Start: stamp(base + 37), End: stamp(end), Step: promapi.NewOptString("15s"),
		})
		require.NoError(t, err)
		require.Equal(t, promapi.MatrixData, result.Data.Type)
		require.Len(t, result.Data.Matrix.Result, 1)
		points := result.Data.Matrix.Result[0].Values
		require.Len(t, points, int((end-base-37)/15)+1)
		for _, point := range points {
			require.InDelta(t, 4, point.V, 1e-12)
		}
		count, err := client.GetQuery(ctx, promapi.GetQueryParams{Query: `count(sparse_paged_cpu_seconds_total)`, Time: promapi.NewOptPrometheusTimestamp(stamp(end))})
		require.NoError(t, err)
		require.Equal(t, promapi.VectorData, count.Data.Type)
		require.Len(t, count.Data.Vector.Result, 1)
		value, ok := count.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
		require.True(t, ok)
		require.EqualValues(t, 4096, value)
		selector := `sparse_paged_cpu_seconds_total{instance="host-0",mode=~"idle|system",cpu!="3"}`
		start, finish := promapi.NewOptPrometheusTimestamp(stamp(base)), promapi.NewOptPrometheusTimestamp(stamp(end))
		series, err := client.GetSeries(ctx, promapi.GetSeriesParams{Match: []string{selector}, Start: start, End: finish})
		require.NoError(t, err)
		require.Len(t, series.Data, 4)
		modes, err := client.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: "mode", Match: []string{selector}, Start: start, End: finish})
		require.NoError(t, err)
		require.Equal(t, []string{"idle", "system"}, []string(modes.Data))
		labels, err := client.GetLabels(ctx, promapi.GetLabelsParams{Match: []string{selector}, Start: start, End: finish})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"__name__", "cpu", "instance", "job", "mode"}, []string(labels.Data))
		cardinality := backend.Store().Cardinality("default", signal.Metric, 5)
		require.Equal(t, "resident_head", cardinality.IndexScope, "the config flag must reach the engine")
		require.True(t, cardinality.TotalSeriesComplete)
		require.EqualValues(t, 4096, cardinality.TotalSeries)
	}
	for round := range 2 {
		write(round)
		require.NoError(t, backend.Store().Admin().Flush(ctx, "default", signal.Metric))
	}
	t.Run("parts", func(t *testing.T) { check(t, base+142) })
	write(2)
	t.Run("head-and-parts", func(t *testing.T) { check(t, base+217) })
	require.NoError(t, closeStore(ctx))
	backend, closeStore = open()
	t.Run("reopened", func(t *testing.T) { check(t, base+217) })
	stats := backend.Inspect()
	require.Len(t, stats.Tenants, 1)
	require.Len(t, stats.Tenants[0].Signals, 1)
	require.Zero(t, stats.Tenants[0].Signals[0].HeadItems)
	require.True(t, stats.Tenants[0].Signals[0].SeriesComplete)
	require.EqualValues(t, 4096, stats.Tenants[0].Signals[0].Series)
}
