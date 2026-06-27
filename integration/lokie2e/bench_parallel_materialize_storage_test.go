package lokie2e_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkLogQLMaterializeParallel measures a materialization-heavy log query (every record
// survives, so the cost is dominated by building per-record label sets) with the sequential path
// (parallelism off) versus the opt-in parallel materializer.
func BenchmarkLogQLMaterializeParallel(b *testing.B) {
	const records = 50_000
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	build := func(parallelism int) *logqlengine.Engine {
		ctx := context.Background()
		store, err := storage.InMemory()
		require.NoError(b, err)
		b.Cleanup(func() { _ = store.Close(ctx) })
		backend := storagebackend.New(store, storagebackend.WithLogParallelism(parallelism))

		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		for i := range records {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(now.Add(time.Duration(i) * time.Millisecond).UnixNano()))
			lr.Body().SetStr(fmt.Sprintf("request %d completed", i))
			lr.Attributes().PutStr("env", "bench")
		}
		require.NoError(b, backend.ConsumeLogs(ctx, ld))

		engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
			ParseOptions: logql.ParseOptions{AllowDots: true},
		})
		require.NoError(b, err)
		return engine
	}

	params := logqlengine.EvalParams{
		Start:     now.Add(-time.Hour),
		End:       now.Add(time.Hour),
		Direction: logqlengine.DirectionForward,
		Limit:     records,
	}

	for _, mode := range []struct {
		name        string
		parallelism int
	}{
		{"Sequential", 0},
		{fmt.Sprintf("Parallel-%d", runtime.GOMAXPROCS(0)), runtime.GOMAXPROCS(0)},
	} {
		engine := build(mode.parallelism)
		query, err := engine.NewQuery(context.Background(), `{env="bench"}`)
		require.NoError(b, err)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := query.Eval(context.Background(), params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
