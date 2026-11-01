package prome2e_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestScarecrowBorrowedWindowsNativeFileHTTP(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			files, err := backendfile.New(filepath.Join(dir, "parts"))
			require.NoError(t, err)
			store, err := storage.Open(t.Context(), storage.Options{}, storage.WithBackend(files),
				storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
			backend := storagebackend.New(store)
			base := time.Unix(2_000_000, 0).UTC()
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("window_gauge")
			gauge := metric.SetEmptyGauge()
			for series, factor := range map[string]float64{"a": 1, "b": 2} {
				for i, v := range []float64{9, 1, 7, 3, 8} {
					point := gauge.DataPoints().AppendEmpty()
					point.Attributes().PutStr("instance", series)
					point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * 15 * time.Second)))
					point.SetDoubleValue(v * factor)
				}
			}
			require.NoError(t, backend.ConsumeMetrics(t.Context(), metrics))
			if state == "parts" {
				require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
				require.NoError(t, store.Close(t.Context()))
				store, err = storage.Open(t.Context(), storage.Options{}, storage.WithBackend(files),
					storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
				require.NoError(t, err)
				backend = storagebackend.New(store)
			}
			handler, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{Querier: backend})
			require.NoError(t, err)
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
			require.NoError(t, err)
			for _, tt := range []struct {
				query string
				want  []float64
			}{
				{`quantile_over_time(0.5, window_gauge[30s])`, []float64{4, 5, 5.5}},
				{`irate(window_gauge[30s])`, []float64{0.4, 0.2, 1.0 / 3}},
				{`window_gauge`, []float64{7, 3, 8}},
			} {
				t.Run(tt.query, func(t *testing.T) {
					response, err := client.GetQueryRange(t.Context(), promapi.GetQueryRangeParams{
						Query: tt.query,
						Start: getPromTS(pcommon.NewTimestampFromTime(base.Add(30 * time.Second))),
						End:   getPromTS(pcommon.NewTimestampFromTime(base.Add(time.Minute))),
						Step:  promapi.NewOptString("15s"),
					})
					require.NoError(t, err)
					require.Equal(t, promapi.MatrixData, response.Data.Type)
					require.Len(t, response.Data.Matrix.Result, 2)
					for _, series := range response.Data.Matrix.Result {
						factor, ok := map[string]float64{"a": 1, "b": 2}[series.Metric["instance"]]
						require.True(t, ok)
						require.Len(t, series.Values, len(tt.want))
						for i, point := range series.Values {
							require.InDelta(t, tt.want[i]*factor, point.V, 1e-12)
						}
					}
				})
			}
		})
	}
}
