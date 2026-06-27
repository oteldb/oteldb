package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkLogQLStorage benchmarks representative real-world LogQL queries end-to-end against the
// embedded storage engine: a realistic set of HTTP access logs (thousands of records across several
// streams) is ingested through the storage sink, then each query is evaluated through the LogQL
// engine over the storage fetch seam — log selectors, line filters, label filters, and metric
// queries (count_over_time/rate/sum by).
func BenchmarkLogQLStorage(b *testing.B) {
	ctx := context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 50) // ~6k access-log records across testService/fooService streams
	require.NoError(b, err)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	for _, batch := range set.Batches {
		require.NoError(b, backend.ConsumeLogs(ctx, batch))
	}

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		ParseOptions: logql.ParseOptions{AllowDots: true},
	})
	require.NoError(b, err)

	params := logqlengine.EvalParams{
		Start:     set.Start.AsTime(),
		End:       set.End.AsTime(),
		Step:      15 * time.Second,
		Direction: logqlengine.DirectionForward,
		Limit:     1000,
	}

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"StreamSelector", `{service_name="testService"}`},
		{"LineFilterInclude", `{service_name=~".+"} |= "GET"`},
		{"LineFilterExclude", `{service_name=~".+"} != "HEAD"`},
		{"LabelFilter", `{service_name=~".+"} | http_method = "POST"`},
		{"CountOverTime", `count_over_time({service_name=~".+"}[1m])`},
		{"Rate", `rate({service_name=~".+"}[1m])`},
		{"SumByMethod", `sum by (http_method) (count_over_time({service_name=~".+"}[1m]))`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			query, err := engine.NewQuery(ctx, tt.query)
			require.NoError(b, err)

			// Sanity-check the query is meaningful (non-empty) before timing.
			data, err := query.Eval(ctx, params)
			require.NoError(b, err)
			require.True(b, queryDataNonEmpty(data), "query %q returned no data", tt.query)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := query.Eval(ctx, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func queryDataNonEmpty(data lokiapi.QueryResponseData) bool {
	if s, ok := data.GetStreamsResult(); ok {
		return len(s.Result) > 0
	}
	if m, ok := data.GetMatrixResult(); ok {
		return len(m.Result) > 0
	}
	if v, ok := data.GetVectorResult(); ok {
		return len(v.Result) > 0
	}
	return false
}
