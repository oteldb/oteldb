package prome2e_test

import (
	"context"
	"math"
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

func TestScarecrowMadNaNNativeHTTP(t *testing.T) {
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			backend := storagebackend.New(store)
			base := time.Unix(2_000_000, 0).UTC()
			md := pmetric.NewMetrics()
			metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("mad_gauge")
			gauge := metric.SetEmptyGauge()
			for i, v := range []float64{1, 2, math.NaN(), 4, 5} {
				point := gauge.DataPoints().AppendEmpty()
				point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i-3) * 10 * time.Second)))
				point.SetDoubleValue(v)
			}
			require.NoError(t, backend.ConsumeMetrics(t.Context(), md))
			if state == "parts" {
				require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
			}
			engine := scarecrow.NewEngine(scarecrow.Opts{
				Parser: parser.Options{EnableExperimentalFunctions: true},
				NewScanner: func(promstorage.Queryable) scarecrow.Scanner {
					return backend.ScarecrowScanner()
				},
			})
			handler, err := promapi.NewServer(promhandler.NewPromAPI(engine, backend, backend, backend, promhandler.PromAPIOptions{}))
			require.NoError(t, err)
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := promapi.NewClient(server.URL, promapi.WithClient(server.Client()))
			require.NoError(t, err)
			response, err := client.GetQueryRange(t.Context(), promapi.GetQueryRangeParams{
				Query: "mad_over_time(mad_gauge[30s])",
				Start: getPromTS(pcommon.NewTimestampFromTime(base)),
				End:   getPromTS(pcommon.NewTimestampFromTime(base.Add(20 * time.Second))),
				Step:  promapi.NewOptString("10s"),
			})
			require.NoError(t, err)
			require.Equal(t, promapi.MatrixData, response.Data.Type)
			require.Len(t, response.Data.Matrix.Result, 1)
			values := response.Data.Matrix.Result[0].Values
			require.Len(t, values, 3)
			require.True(t, math.IsNaN(values[0].V))
			require.True(t, math.IsNaN(values[1].V))
			require.Equal(t, 0.5, values[2].V)
		})
	}
}
