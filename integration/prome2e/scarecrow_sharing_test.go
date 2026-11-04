package prome2e_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
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

func TestScarecrowSharingNativeFileHTTP(t *testing.T) {
	for _, state := range []string{"head", "reopened-parts"} {
		t.Run(state, func(t *testing.T) {
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
			metric.SetName("sharing_cpu_seconds")
			points := metric.SetEmptyGauge().DataPoints()
			for mode := range 6 {
				for cpu := range 2 {
					for j := range 41 {
						point := points.AppendEmpty()
						point.Attributes().PutStr("mode", strconv.Itoa(mode))
						point.Attributes().PutStr("cpu", strconv.Itoa(cpu))
						point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(j) * 15 * time.Second)))
						point.SetDoubleValue(float64(j * 15 * (mode + 1)))
					}
				}
			}
			require.NoError(t, backend.ConsumeMetrics(t.Context(), metrics))
			if state == "reopened-parts" {
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
			params := func(mode int) promapi.GetQueryRangeParams {
				return promapi.GetQueryRangeParams{
					Query: `sum(irate(sharing_cpu_seconds{mode="` + strconv.Itoa(mode) + `"}[1m])) / scalar(count(count(sharing_cpu_seconds) by(cpu)))`,
					Start: getPromTS(pcommon.NewTimestampFromTime(base.Add(5 * time.Minute))),
					End:   getPromTS(pcommon.NewTimestampFromTime(base.Add(10 * time.Minute))),
					Step:  promapi.NewOptString("15s"),
				}
			}
			var wg sync.WaitGroup
			for mode := range 6 {
				wg.Go(func() {
					response, err := client.GetQueryRange(t.Context(), params(mode))
					require.NoError(t, err)
					require.Len(t, response.Data.Matrix.Result, 1)
					require.Len(t, response.Data.Matrix.Result[0].Values, 21)
					for _, point := range response.Data.Matrix.Result[0].Values {
						require.InDelta(t, float64(mode+1), point.V, 1e-12)
					}
				})
			}
			wg.Wait()
			// A late write changes an already queried window. A completed result must not conceal it.
			late := pmetric.NewMetrics()
			m := late.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("sharing_cpu_seconds")
			point := m.SetEmptyGauge().DataPoints().AppendEmpty()
			point.Attributes().PutStr("mode", "0")
			point.Attributes().PutStr("cpu", "0")
			point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(599 * time.Second)))
			point.SetDoubleValue(100000)
			require.NoError(t, backend.ConsumeMetrics(t.Context(), late))
			response, err := client.GetQueryRange(t.Context(), params(0))
			require.NoError(t, err)
			require.Len(t, response.Data.Matrix.Result, 1)
			values := response.Data.Matrix.Result[0].Values
			require.InDelta(t, 300.5, values[len(values)-1].V, 1e-12)
		})
	}
}
