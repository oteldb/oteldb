package prome2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkInserterMetricsStorage measures the overhead of ingesting metrics into the embedded
// storage engine through the OTLP ingestion sink, mirroring the inserter benchmarks of the other
// signals (lokie2e/BenchmarkInserterLogs, tempoe2e/BenchmarkInserterTraces).
func BenchmarkInserterMetricsStorage(b *testing.B) {
	ctx := context.Background()

	set, err := readBatchSet("_testdata/metrics.json")
	require.NoError(b, err)
	require.NotEmpty(b, set.Batches)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		for _, batch := range set.Batches {
			sinkErr = backend.ConsumeMetrics(ctx, batch)
		}
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}
