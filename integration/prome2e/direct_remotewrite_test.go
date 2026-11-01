package prome2e_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	prwpb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestDirectRemoteWriteFileBackend(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	open := func() *storage.Storage {
		store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(files),
			storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
		require.NoError(t, err)
		return store
	}
	store := open()
	t.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	handler := promrw.NewHandler(backend, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: 100 * 365 * 24 * time.Hour},
	})
	write := func() {
		for _, host := range []struct {
			name  string
			value float64
		}{{"host-1", 7}, {"host-2", 11}} {
			raw, err := (&prwpb.WriteRequest{Timeseries: []prwpb.TimeSeries{{
				Labels:  []prwpb.Label{{Name: "__name__", Value: "direct_metric"}, {Name: "instance", Value: host.name}},
				Samples: []prwpb.Sample{{Timestamp: 1700000000000, Value: host.value}},
			}}}).Marshal()
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("Content-Encoding", "snappy")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
		}
	}
	write()
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	write()
	check := func(t *testing.T) {
		t.Helper()
		_, client := setupDB(t, integration.TraceProvider(t), storagebackend.New(store))
		for _, tt := range []struct {
			name, query string
			want        float64
		}{
			{"count", "count(direct_metric)", 2},
			{"sum", "sum(direct_metric)", 18},
			{"first request retained", `direct_metric{instance="host-1"}`, 7},
			{"second request retained", `direct_metric{instance="host-2"}`, 11},
			{"finite host set", `sum(direct_metric{instance=~"host-1|host-2"})`, 18},
			{"finite host set with missing value", `sum(direct_metric{instance=~"host-1|absent"})`, 7},
		} {
			t.Run(tt.name, func(t *testing.T) {
				result, err := client.GetQuery(ctx, promapi.GetQueryParams{
					Query: tt.query, Time: promapi.NewOptPrometheusTimestamp("1700000000"),
				})
				require.NoError(t, err)
				require.Len(t, result.Data.Vector.Result, 1)
				value, ok := result.Data.Vector.Result[0].Value.HistogramOrValue.GetStringFloat64()
				require.True(t, ok)
				require.Equal(t, tt.want, value)
			})
		}
		t.Run("range grouped counts", func(t *testing.T) {
			result, err := client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
				Query: `count(count(direct_metric{instance=~"host-1|host-2"}) by(instance))`,
				Start: "1700000000", End: "1700000060", Step: promapi.NewOptString("15s"),
			})
			require.NoError(t, err)
			require.Len(t, result.Data.Matrix.Result, 1)
			require.Len(t, result.Data.Matrix.Result[0].Values, 5)
			for _, point := range result.Data.Matrix.Result[0].Values {
				require.Equal(t, 2.0, point.V)
			}
		})
	}
	t.Run("head and parts", check)
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
	require.NoError(t, store.Close(ctx))
	store = open()
	t.Run("reopened", check)
}
