package prome2e_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestScarecrowUnaryKernelsNativeHTTP(t *testing.T) {
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
			backend := storagebackend.New(store)
			base := time.Unix(2_000_000, 0).UTC()
			md := pmetric.NewMetrics()
			metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("kernel_gauge")
			gauge := metric.SetEmptyGauge()
			for i := range 241 {
				point := gauge.DataPoints().AppendEmpty()
				point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
				point.SetDoubleValue(-float64(i))
			}
			require.NoError(t, backend.ConsumeMetrics(t.Context(), md))
			if state == "parts" {
				require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
			}
			engine := scarecrow.NewEngine(scarecrow.Opts{
				Parser:     parser.Options{EnableExperimentalFunctions: true},
				NewScanner: func(promstorage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() },
			})
			handler, err := promapi.NewServer(promhandler.NewPromAPI(engine, backend, backend, backend, promhandler.PromAPIOptions{}))
			require.NoError(t, err)
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
			require.NoError(t, err)
			for _, tc := range []struct {
				name, query string
				start       int
				value       func(int) float64
			}{
				{"abs", "abs(kernel_gauge)", 0, func(i int) float64 { return float64(i) }},
				{"mad", "mad_over_time(kernel_gauge[64s])", 100, func(int) float64 { return 16 }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					response, err := client.GetQueryRange(t.Context(), promapi.GetQueryRangeParams{
						Query: tc.query,
						Start: getPromTS(pcommon.NewTimestampFromTime(base.Add(time.Duration(tc.start) * time.Second))),
						End:   getPromTS(pcommon.NewTimestampFromTime(base.Add(240 * time.Second))),
						Step:  promapi.NewOptString("1s"),
					})
					require.NoError(t, err)
					require.Equal(t, promapi.MatrixData, response.Data.Type)
					require.Len(t, response.Data.Matrix.Result, 1)
					result := response.Data.Matrix.Result[0]
					require.NotContains(t, result.Metric, "__name__")
					require.Len(t, result.Values, 241-tc.start)
					for i, p := range result.Values {
						require.Equal(t, tc.value(tc.start+i), p.V, "step %d", i)
					}
				})
			}
		})
	}
}
