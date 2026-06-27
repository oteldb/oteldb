package storagebackend_test

import (
	"context"
	"fmt"
	"sort"
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

// parallelTestData ingests a dataset that exercises both materialization shapes:
//   - one big empty-resource stream (Loki-style) of bigStream records, so chunk boundaries fall
//     inside a single batch;
//   - several resource streams whose timestamps overlap the big stream, so the merged result has
//     ties that must be ordered identically regardless of worker scheduling.
//
// Every record carries env="test" so a single selector matches all of them.
func parallelTestData(tb testing.TB, parallelism int) (backend *storagebackend.Backend, start, end time.Time) {
	tb.Helper()
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = store.Close(ctx) })
	backend = storagebackend.New(store, storagebackend.WithLogParallelism(parallelism))

	base := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	const bigStream = 3000

	// Big empty-resource stream.
	{
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		for i := range bigStream {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
			lr.Body().SetStr(fmt.Sprintf("big %d", i))
			lr.Attributes().PutStr("env", "test")
			lr.Attributes().PutStr("job", fmt.Sprintf("job-%d", i%10))
		}
		require.NoError(tb, backend.ConsumeLogs(ctx, ld))
	}
	// Several resource streams with timestamps overlapping the big stream (forces ties).
	for s := range 5 {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", s))
		sl := rl.ScopeLogs().AppendEmpty()
		for j := range 100 {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(j) * time.Millisecond).UnixNano()))
			lr.Body().SetStr(fmt.Sprintf("svc%d %d", s, j))
			lr.Attributes().PutStr("env", "test")
		}
		require.NoError(tb, backend.ConsumeLogs(ctx, ld))
	}

	return backend, base.Add(-time.Hour), base.Add(time.Hour)
}

// resultRows drains a query into a fully-ordered, comparable representation: one row per entry in
// result order, capturing timestamp, line, and the sorted label set. Any divergence in selection,
// order (including tie order), or labels between two runs shows up as a row difference.
func resultRows(tb testing.TB, b *storagebackend.Backend, sel []logql.LabelMatcher, params logqlengine.EvalParams) []string {
	tb.Helper()
	node, err := b.Logs().Query(context.Background(), sel)
	require.NoError(tb, err)
	it, err := node.EvalPipeline(context.Background(), params)
	require.NoError(tb, err)

	var rows []string
	var e logqlengine.Entry
	for it.Next(&e) {
		var labels []string
		e.Set.Range(func(k logql.Label, v pcommon.Value) {
			labels = append(labels, fmt.Sprintf("%s=%s", k, v.AsString()))
		})
		sort.Strings(labels)
		rows = append(rows, fmt.Sprintf("%d|%s|%v", e.Timestamp, e.Line, labels))
	}
	require.NoError(tb, it.Err())
	return rows
}

// TestLogMaterializeParallelEquivalence proves the parallel materializer returns byte-for-byte the
// same result as the sequential one, across data shapes, parallelism degrees, selectivity, limits,
// and directions. Run with -race to also prove the workers are data-race free.
func TestLogMaterializeParallelEquivalence(t *testing.T) {
	t.Parallel()

	parallels := []int{1, 2, 3, 7, 8, 64, 4096} // 4096 > total records: workers clamp to record count
	backends := make(map[int]*storagebackend.Backend, len(parallels)+1)
	seq, start, end := parallelTestData(t, 0) // sequential reference
	backends[0] = seq
	for _, p := range parallels {
		b, _, _ := parallelTestData(t, p)
		backends[p] = b
	}

	eq := func(label, value string) []logql.LabelMatcher {
		return []logql.LabelMatcher{{Label: logql.Label(label), Op: logql.OpEq, Value: value}}
	}
	re := func(label, value string) []logql.LabelMatcher {
		return []logql.LabelMatcher{{Label: logql.Label(label), Op: logql.OpRe, Value: value}}
	}

	cases := []struct {
		name   string
		sel    []logql.LabelMatcher
		params logqlengine.EvalParams
	}{
		{"AllRecords", eq("env", "test"), logqlengine.EvalParams{Start: start, End: end, Limit: -1}},
		{"Selective", eq("job", "job-3"), logqlengine.EvalParams{Start: start, End: end, Limit: -1}},
		{"ResourceStream", eq("service_name", "svc-2"), logqlengine.EvalParams{Start: start, End: end, Limit: -1}},
		{"Empty", eq("env", "absent"), logqlengine.EvalParams{Start: start, End: end, Limit: -1}},
		{"ForwardLimit", eq("env", "test"), logqlengine.EvalParams{Start: start, End: end, Direction: logqlengine.DirectionForward, Limit: 17}},
		{"BackwardLimit", eq("env", "test"), logqlengine.EvalParams{Start: start, End: end, Direction: logqlengine.DirectionBackward, Limit: 23}},
		{"RegexAll", re("env", "te.*"), logqlengine.EvalParams{Start: start, End: end, Limit: -1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := resultRows(t, backends[0], tc.sel, tc.params)
			for _, p := range parallels {
				require.Equalf(t, want, resultRows(t, backends[p], tc.sel, tc.params),
					"parallelism %d must equal sequential", p)
			}
		})
	}
}
