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

// BenchmarkLogQLRecordAttrSelector measures a selector on a clean record-attribute label over a
// single high-cardinality stream (the Loki shape: stream labels arrive as per-record attributes,
// empty resource). The embedded querier offloads the equality to a per-record fetch condition, so
// only the matching records are materialized into entries with label sets instead of all of them.
func BenchmarkLogQLRecordAttrSelector(b *testing.B) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)

	const (
		jobs   = 20
		perJob = 300
	)
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty() // empty resource: every record lands in one stream
	sl := rl.ScopeLogs().AppendEmpty()
	for j := range jobs {
		for r := range perJob {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(now.Add(time.Duration(r) * time.Millisecond).UnixNano()))
			lr.Body().SetStr(fmt.Sprintf("GET /%d 200", r))
			lr.Attributes().PutStr("job", fmt.Sprintf("job-%02d", j))
		}
	}
	require.NoError(b, backend.ConsumeLogs(ctx, ld))

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		ParseOptions: logql.ParseOptions{AllowDots: true},
	})
	require.NoError(b, err)

	params := logqlengine.EvalParams{
		Start:     now.Add(-time.Hour),
		End:       now.Add(time.Hour),
		Direction: logqlengine.DirectionForward,
		Limit:     10000,
	}
	query, err := engine.NewQuery(ctx, `{job="job-07"}`)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := query.Eval(ctx, params); err != nil {
			b.Fatal(err)
		}
	}
}
