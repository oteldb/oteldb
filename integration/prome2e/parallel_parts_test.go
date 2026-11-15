package prome2e_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
	prwpb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestNativeParallelPartsHTTPReopen(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.WithoutCancel(ctx))) })
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{
			MetricDiskSeries: true, MetricSeriesChunks: true, MetricResidentHeadBytes: 32 << 20,
			MergeMemoryBytes: 128 << 20,
		}, storage.WithBackend(files), storage.WithWALDir(filepath.Join(dir, "wal")),
			storage.WithFlushInterval(-1), storage.WithFlushThresholdBytes(-1), storage.WithMeterProvider(mp),
			storage.WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
				return tenant.Policy{Limits: tenant.Limits{MaxPartSize: 32 * 32}}
			})))
		require.NoError(t, err)
		return store
	}
	store := open()
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(ctx))) })
	const hosts, base = 128, 1700000000
	request := prwpb.WriteRequest{}
	for host := range hosts {
		series := prwpb.TimeSeries{Labels: []prwpb.Label{
			{Name: "__name__", Value: "parallel_part_metric"}, {Name: "instance", Value: "host-" + strconv.Itoa(host)},
		}}
		for point := range 4 {
			series.Samples = append(series.Samples, prwpb.Sample{Timestamp: int64(base+15*point) * 1000, Value: float64(host + point)})
		}
		request.Timeseries = append(request.Timeseries, series)
	}
	raw, err := request.Marshal()
	require.NoError(t, err)
	handler := promrw.NewHandler(storagebackend.New(store), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &collected))
	var buildCount uint64
	var workers int64
	memory := map[string]metricdata.HistogramDataPoint[int64]{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "storage.flush.build.memory.bytes" {
				h, ok := m.Data.(metricdata.Histogram[int64])
				require.True(t, ok)
				for _, point := range h.DataPoints {
					component, ok := point.Attributes.Value(attribute.Key("component"))
					require.True(t, ok)
					memory[component.AsString()] = point
				}
			}
			if m.Name != "storage.flush.build.workers" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, point := range h.DataPoints {
				buildCount, workers = buildCount+point.Count, workers+point.Sum
			}
		}
	}
	require.EqualValues(t, 1, buildCount)
	require.EqualValues(t, 2, workers, "the production facade must admit two native builders")
	require.EqualValues(t, 1, memory["held"].Count)
	require.Positive(t, memory["held"].Sum)
	require.LessOrEqual(t, memory["held"].Sum, int64(32<<20), "two builders must fit one flush allowance")
	require.Less(t, memory["additional"].Sum, memory["held"].Sum, "request only missing credit")
	require.GreaterOrEqual(t, memory["job"].Count, uint64(2))
	require.EqualValues(t, 2, memory["checked_private"].Count)
	require.EqualValues(t, 2, memory["tracked_private"].Count)
	require.Positive(t, memory["tracked_private"].Sum)
	check := func(t *testing.T) {
		t.Helper()
		_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
		result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
			Query: "sum(parallel_part_metric)", Start: "1700000000", End: "1700000045", Step: promapi.NewOptString("15s"),
		})
		require.NoError(t, err)
		require.Len(t, result.Data.Matrix.Result, 1)
		points := result.Data.Matrix.Result[0].Values
		require.Len(t, points, 4)
		for i, point := range points {
			require.EqualValues(t, hosts*(hosts-1)/2+hosts*i, point.V)
		}
	}
	t.Run("parts", check)
	require.NoError(t, store.Close(ctx))
	store = open()
	t.Run("reopened", check)
}
