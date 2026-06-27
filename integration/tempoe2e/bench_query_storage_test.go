package tempoe2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// BenchmarkTraceQLStorageSuite benchmarks representative real-world TraceQL queries end-to-end
// against the embedded storage engine: a captured trace corpus is ingested through the storage
// sink, then each query is evaluated through the TraceQL engine over the storage fetch seam —
// attribute filters, intrinsics, boolean combinations, and a scalar pipeline.
func BenchmarkTraceQLStorageSuite(b *testing.B) {
	ctx := context.Background()

	set, err := readBatchSet()
	require.NoError(b, err)
	require.NotEmpty(b, set.Batches)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	for _, batch := range set.Batches {
		require.NoError(b, backend.ConsumeTraces(ctx, batch))
	}

	engine := traceqlengine.NewEngine(backend.Traces(), traceqlengine.Options{})
	params := traceqlengine.EvalParams{Limit: 200}

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"AttrFilter", `{ .http.method = "POST" }`},
		{"AttrAndStatus", `{ .http.method = "POST" && .http.status_code = 200 }`},
		{"SpanName", `{ name = "list-articles" }`},
		{"DurationIntrinsic", `{ duration > 0ns }`},
		{"RegexAttr", `{ .http.method =~ "POST|GET" }`},
		{"ScalarPipeline", `{ .http.method = "POST" } | count() > 0`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			// Sanity-check the query is meaningful (non-empty) before timing.
			res, err := engine.Eval(ctx, tt.query, params)
			require.NoError(b, err)
			require.NotEmptyf(b, res.Traces, "query %q returned no traces", tt.query)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := engine.Eval(ctx, tt.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
