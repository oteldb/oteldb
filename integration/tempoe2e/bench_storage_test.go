package tempoe2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// newStorageBackend opens an in-memory embedded storage engine for benchmarks.
func newStorageBackend(b *testing.B) (*storagebackend.Backend, context.Context) {
	b.Helper()
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	return storagebackend.New(store), ctx
}

// BenchmarkInserterTracesStorage measures the overhead of ingesting traces into the embedded
// storage engine, mirroring BenchmarkInserterTraces (which targets the ClickHouse inserter).
func BenchmarkInserterTracesStorage(b *testing.B) {
	set, err := readBatchSet()
	require.NoError(b, err)

	backend, ctx := newStorageBackend(b)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		for _, batch := range set.Batches {
			sinkErr = backend.ConsumeTraces(ctx, batch)
		}
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}

// BenchmarkTraceQLStorage measures TraceQL evaluation against the embedded storage engine,
// mirroring BenchmarkTraceQL (which uses the in-memory reference querier).
func BenchmarkTraceQLStorage(b *testing.B) {
	set, err := readBatchSet()
	require.NoError(b, err)

	backend, ctx := newStorageBackend(b)
	for _, batch := range set.Batches {
		require.NoError(b, backend.ConsumeTraces(ctx, batch))
	}

	engine := traceqlengine.NewEngine(backend.Traces(), traceqlengine.Options{})

	b.ReportAllocs()
	b.ResetTimer()

	var result *tempoapi.Traces
	for i := 0; i < b.N; i++ {
		result, err = engine.Eval(ctx, `{ .http.method = "POST" && .http.status_code = 200 }`, traceqlengine.EvalParams{Limit: 20})
		if err != nil {
			b.Fatal(err)
		}
	}

	if result == nil || len(result.Traces) < 1 {
		b.Fatal("empty result")
	}
}
