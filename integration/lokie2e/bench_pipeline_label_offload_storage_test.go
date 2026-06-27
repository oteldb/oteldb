package lokie2e_test

import (
	"context"
	"fmt"
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

// BenchmarkLogQLPipelineLabelFilter measures a pipeline label filter (`| level="..."`) on a clean
// record-attribute label over one stream. LogQLOptimizer extracts the filter and the querier
// offloads it to a per-record fetch condition, so only matching records are materialized.
func BenchmarkLogQLPipelineLabelFilter(b *testing.B) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	const (
		levels       = 20
		perLevel     = 300
		selectedLeve = "level-07"
	)
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("app", "bench") // stream selector label
	sl := rl.ScopeLogs().AppendEmpty()
	for l := range levels {
		for r := range perLevel {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(now.Add(time.Duration(r) * time.Millisecond).UnixNano()))
			lr.Body().SetStr(fmt.Sprintf("request %d", r))
			lr.Attributes().PutStr("level", fmt.Sprintf("level-%02d", l))
		}
	}
	require.NoError(b, backend.ConsumeLogs(ctx, ld))

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		ParseOptions: logql.ParseOptions{AllowDots: true},
		Optimizers:   []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
	})
	require.NoError(b, err)

	params := logqlengine.EvalParams{
		Start:     now.Add(-time.Hour),
		End:       now.Add(time.Hour),
		Direction: logqlengine.DirectionForward,
		Limit:     10000,
	}
	query, err := engine.NewQuery(ctx, fmt.Sprintf(`{app="bench"} | level = %q`, selectedLeve))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := query.Eval(ctx, params); err != nil {
			b.Fatal(err)
		}
	}
}
