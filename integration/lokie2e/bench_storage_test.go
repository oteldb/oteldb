package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkInserterLogsStorage measures the overhead of ingesting logs into the embedded
// storage engine, mirroring BenchmarkInserterLogs (which targets the ClickHouse inserter).
func BenchmarkInserterLogsStorage(b *testing.B) {
	ctx := context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 10)
	require.NoError(b, err)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		for _, batch := range set.Batches {
			sinkErr = backend.ConsumeLogs(ctx, batch)
		}
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}
