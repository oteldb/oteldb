package storagebackend

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
)

// TestLogFetchLimitPushdown is a white-box check that the ordered top-N is really pushed into the
// storage fetch, and that it is gated on the selector being fully pushed.
//
// The equivalence test in logs_limit_test.go proves the results are right; this one proves the work
// is actually skipped, which a result-level test cannot see.
func TestLogFetchLimitPushdown(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := New(store)

	const records = 500
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	for _, env := range []string{"prod", "stage"} {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "svc")
		rl.Resource().Attributes().PutStr("env", env)
		recs := rl.ScopeLogs().AppendEmpty().LogRecords()
		for i := range records {
			r := recs.AppendEmpty()
			r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
			r.Body().SetStr(fmt.Sprintf("%s %d", env, i))
		}
	}
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lo, hi := fetchWindow(base.Add(-time.Hour), base.Add(time.Hour))
	node := func(matchers ...logql.LabelMatcher) *logStreamNode {
		t.Helper()
		n, err := b.Logs().Query(ctx, matchers)
		require.NoError(t, err)
		sn, ok := n.(*logStreamNode)
		require.True(t, ok)
		return sn
	}
	rows := func(n *logStreamNode, opts fetchOptions) (int, bool) {
		t.Helper()
		batches, pushed, err := n.fetchBatches(ctx, lo, hi, opts)
		require.NoError(t, err)
		total := 0
		for _, batch := range batches {
			total += len(batch.Timestamps)
		}
		return total, pushed
	}

	pushable := node(logql.LabelMatcher{Label: "env", Op: logql.OpEq, Value: "prod"})
	// `env!="dev"` also matches a stream without env, so it cannot be pushed to the index; the
	// post-fetch matchSelector must re-check it, and a fetch-side limit would be unsound.
	unpushable := node(logql.LabelMatcher{Label: "env", Op: logql.OpNotEq, Value: "dev"})

	t.Run("unlimited", func(t *testing.T) {
		got, pushed := rows(pushable, fetchOptions{})
		require.True(t, pushed)
		require.Equal(t, records, got)
	})
	t.Run("limited", func(t *testing.T) {
		// The fetch keeps boundary ties, so it may return a superset of the limit — but far less
		// than the whole window.
		got, pushed := rows(pushable, fetchOptions{limit: 10, reverse: true})
		require.True(t, pushed)
		require.GreaterOrEqual(t, got, 10)
		require.Less(t, got, records)
	})
	t.Run("newest when reverse", func(t *testing.T) {
		batches, _, err := pushable.fetchBatches(ctx, lo, hi, fetchOptions{limit: 1, reverse: true})
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Equal(t, []int64{base.Add((records - 1) * time.Millisecond).UnixNano()}, batches[0].Timestamps)
	})
	t.Run("oldest when forward", func(t *testing.T) {
		batches, _, err := pushable.fetchBatches(ctx, lo, hi, fetchOptions{limit: 1})
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Equal(t, []int64{base.UnixNano()}, batches[0].Timestamps)
	})
	t.Run("not pushed when selector is not fully pushed", func(t *testing.T) {
		got, pushed := rows(unpushable, fetchOptions{limit: 10, reverse: true})
		require.False(t, pushed)
		require.Equal(t, 2*records, got, "the limit must be ignored: matchSelector still filters")
	})
}
