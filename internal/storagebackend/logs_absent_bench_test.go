package storagebackend

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

func BenchmarkLogAbsentSelector(b *testing.B) {
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(context.Background()) })
	backend := New(store)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	for i := range 64 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "svc-"+strconv.Itoa(i))
		if i != 63 {
			rl.Resource().Attributes().PutStr("env", "dev")
		}
		sl := rl.ScopeLogs().AppendEmpty()
		for j := range 64 {
			r := sl.LogRecords().AppendEmpty()
			r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(j) * time.Second).UnixNano()))
			r.Body().SetStr("message")
		}
	}
	require.NoError(b, backend.ConsumeLogs(b.Context(), ld))
	n := &logStreamNode{q: backend.Logs(), selector: []logql.LabelMatcher{{Label: "env", Op: logql.OpNotEq, Value: "dev"}}}
	params := logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Hour), Limit: -1}
	b.ReportAllocs()
	for b.Loop() {
		it, err := n.EvalPipeline(b.Context(), params)
		if err != nil {
			b.Fatal(err)
		}
		var entry logqlengine.Entry
		rows := 0
		for it.Next(&entry) {
			rows++
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if rows != 64 {
			b.Fatalf("got %d rows", rows)
		}
	}
}
