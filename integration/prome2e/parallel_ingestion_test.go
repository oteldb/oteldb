package prome2e_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/golang/snappy"
	prwpb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/xbytes"
)

func TestNativeConcurrentIngestionPressureReopen(t *testing.T) {
	t.Parallel()
	for _, generations := range []int{1, 2} {
		t.Run(strconv.Itoa(generations), func(t *testing.T) {
			t.Parallel()
			testNativeConcurrentIngestionPressureReopen(t, generations, false)
		})
	}
	t.Run("shared-series-two", func(t *testing.T) {
		t.Parallel()
		testNativeConcurrentIngestionPressureReopen(t, 2, true)
	})
	t.Run("sparse-chunks-packed-wal", func(t *testing.T) {
		t.Parallel()
		testNativeConcurrentIngestionPressureReopen(t, 2, true, func(cfg *storagebackend.Config) {
			cfg.MetricSparseChunks = true
			cfg.MetricPackedWAL = true
			cfg.AggregateStats = new(false)
		})
	})
	t.Run("sparse-chunks-wal-off", func(t *testing.T) {
		t.Parallel()
		testNativeConcurrentIngestionPressureReopen(t, 2, true, func(cfg *storagebackend.Config) {
			cfg.MetricSparseChunks = true
			cfg.WALSync = "off"
			cfg.AggregateStats = new(false)
		})
	})
}

func testNativeConcurrentIngestionPressureReopen(
	t *testing.T, generations int, separate bool, configure ...func(*storagebackend.Config),
) {
	t.Helper()
	ctx := t.Context()
	cfg := storagebackend.Config{
		Backend: "file", Dir: t.TempDir(), FlushInterval: time.Hour,
		MetricDiskSeries: true, MetricSeriesChunks: true, MetricAsyncPressureFlush: true,
		MetricPressureGenerations: generations,
		MetricSeparateSeries:      separate,
		MetricResidentHeadBytes:   new(xbytes.Bytes(64 << 10)),
		BuildMemoryBytes:          new(xbytes.Bytes(256 << 20)),
		MergeMemoryBytes:          new(xbytes.Bytes(128 << 20)),
		MetricFlushMemoryBytes:    new(xbytes.Bytes(32 << 20)),
		ReadCacheBytes:            new(xbytes.Bytes(0)), NativeQueryCacheBytes: new(xbytes.Bytes(0)),
	}
	if separate {
		cfg.MetricHeadRotationBytes = new(xbytes.Bytes(16 << 10))
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	open := func() (*storagebackend.Backend, func(context.Context) error) {
		be, closeStore, err := storagebackend.Open(ctx, cfg, zap.NewNop(), &app.Telemetry{})
		require.NoError(t, err)
		return be, closeStore
	}
	be, closeStore := open()
	t.Cleanup(func() { require.NoError(t, closeStore(context.WithoutCancel(ctx))) })
	const workers, perWorker, base = 8, 32, 1700000000
	for round := range 2 {
		handler := promrw.NewHandler(be, promrw.HandlerConfig{
			Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
		})
		var group errgroup.Group
		for worker := range workers {
			group.Go(func() error {
				request := &prwpb.WriteRequest{}
				for i := range perWorker {
					id := worker*perWorker + i
					request.Timeseries = append(request.Timeseries, prwpb.TimeSeries{
						Labels: []prwpb.Label{
							{Name: "__name__", Value: "parallel_metric"},
							{Name: "instance", Value: "host-" + strconv.Itoa(id)},
						},
						Samples: []prwpb.Sample{{Timestamp: (base + int64(round)*15) * 1000, Value: float64(id + round*1000)}},
					})
				}
				raw, err := request.Marshal()
				if err != nil {
					return err
				}
				req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
				req.Header.Set("Content-Type", "application/x-protobuf")
				req.Header.Set("Content-Encoding", "snappy")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, req)
				if response.Code != http.StatusNoContent {
					return errors.Errorf("remote write: status %d: %s", response.Code, response.Body.String())
				}
				return nil
			})
		}
		require.NoError(t, group.Wait())
		check := func(phase string) {
			_, client := setupDB(t, integration.TraceProvider(t), be)
			for _, tc := range []struct {
				query string
				want  float64
			}{
				{query: "count(parallel_metric)", want: workers * perWorker},
				{query: "sum(count_over_time(parallel_metric[30s]))", want: float64((round + 1) * workers * perWorker)},
				{query: "sum(parallel_metric)", want: workers*perWorker*(workers*perWorker-1)/2 + float64(round*1000*workers*perWorker)},
			} {
				result, err := client.GetQuery(ctx, promapi.GetQueryParams{
					Query: tc.query, Time: promapi.NewOptPrometheusTimestamp(promapi.PrometheusTimestamp(strconv.Itoa(base + round*15))),
				})
				require.NoError(t, err, phase)
				require.Len(t, result.Data.Vector.Result, 1)
				value, ok := result.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
				require.True(t, ok)
				require.Equal(t, tc.want, value, phase)
			}
		}
		check("live active/frozen generations")
		require.NoError(t, closeStore(ctx))
		be, closeStore = open()
		check("reopened")
	}
}
