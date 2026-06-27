package prome2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	oteldbpromql "github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkPromQLStorage benchmarks representative real-world PromQL queries end-to-end against the
// embedded storage engine: the captured Prometheus test corpus (~4 MiB of node-exporter-style
// metrics) is ingested through the OTLP storage sink, then each query is executed as a range query
// through the PromQL engine over the storage fetch seam — raw selectors, rate, and aggregations.
func BenchmarkPromQLStorage(b *testing.B) {
	ctx := context.Background()

	set, err := readBatchSet("_testdata/metrics.json")
	require.NoError(b, err)
	require.NotEmpty(b, set.Batches)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	for _, batch := range set.Batches {
		require.NoError(b, backend.ConsumeMetrics(ctx, batch))
	}

	engine, err := oteldbpromql.New(backend, oteldbpromql.EngineOpts{
		MaxSamples:           1_000_000,
		Timeout:              time.Minute,
		LookbackDelta:        5 * time.Minute,
		EnableNegativeOffset: true,
	})
	require.NoError(b, err)

	var (
		start = set.Start.AsTime()
		end   = set.End.AsTime()
		step  = 15 * time.Second
	)

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"Selector", `prometheus_http_requests_total`},
		{"Rate", `rate(prometheus_http_requests_total[5m])`},
		{"SumByHandler", `sum by (handler) (prometheus_http_requests_total)`},
		{"CountSeries", `count(prometheus_http_requests_total)`},
		{"AvgByHandler", `avg by (handler) (prometheus_http_requests_total)`},
		{"Topk", `topk(5, prometheus_http_requests_total)`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			exec := func() *promql.Result {
				q, err := engine.NewRangeQuery(ctx, backend, promql.NewPrometheusQueryOpts(false, 0), tt.query, start, end, step)
				require.NoError(b, err)
				defer q.Close()
				res := q.Exec(ctx)
				require.NoError(b, res.Err)
				return res
			}

			// Sanity-check the query is meaningful (non-empty) before timing.
			res := exec()
			m, ok := res.Value.(promql.Matrix)
			require.Truef(b, ok && len(m) > 0, "query %q returned no series", tt.query)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = exec()
			}
		})
	}
}
