package pyroe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkInserterProfilesStorage measures the overhead of ingesting profiles into the embedded
// storage engine, mirroring the inserter benchmarks of the other signals.
func BenchmarkInserterProfilesStorage(b *testing.B) {
	ctx := context.Background()
	start := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	pd, _, _ := generateProfiles(start, []string{"frontend", "backend"}, 50)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		sinkErr = backend.ConsumeProfiles(ctx, pd)
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}

// BenchmarkProfileQLStorage benchmarks representative real-world ProfileQL queries end-to-end
// against the embedded storage engine: a CPU profiles dataset across several services is ingested
// through the storage sink, then each query is evaluated through the ProfileQL engine over the
// storage fetch seam — a merged flamegraph, a label-filtered flamegraph, and the full flamebearer
// render path.
func BenchmarkProfileQLStorage(b *testing.B) {
	ctx := context.Background()
	backend, start, end, _ := newBackend(b, []string{"frontend", "backend"}, 100)

	engine := profileqlengine.NewEngine(backend.Profiles(), profileqlengine.Options{})
	params := profileqlengine.EvalParams{Start: start.Add(-time.Hour), End: end.Add(time.Hour)}

	b.Run("MergeAll", func(b *testing.B) {
		res, err := engine.Select(ctx, profileTypeID, params)
		require.NoError(b, err)
		require.Positive(b, res.Tree.Total())

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := engine.Select(ctx, profileTypeID, params); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FilterByService", func(b *testing.B) {
		query := profileTypeID + `{service.name="frontend"}`
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := engine.Select(ctx, query, params); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Flamebearer", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := engine.Eval(ctx, profileTypeID, params); err != nil {
				b.Fatal(err)
			}
		}
	})
}
