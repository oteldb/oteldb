package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestLogQLOptimizerBloomPruneSafety is a multi-part regression for the body token hint the optimizer
// attaches to offloaded line filters. Each record is flushed to its own part, so the per-part token
// bloom actually prunes parts before scanning. A matching record whose body glues the filter literal
// into larger tokens ("xGET /a 200y" contains the substring "GET /a 200") must still be returned: an
// unsafe hint would emit "get"/"200", which are absent from that part's word-tokenized bloom, and
// wrongly prune the part — a false negative. SafeTokens keeps only the interior whole token "a",
// present in the body, so the part survives. The optimized result must equal the engine-only result.
func TestLogQLOptimizerBloomPruneSafety(t *testing.T) {
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	b := storagebackend.New(store)

	ts := time.Now().Truncate(time.Second)
	bodies := []string{
		"GET /a 200",        // exact match
		"xGET /a 200y",      // glued on both edges — the over-prune trap
		"POST /b 200",       // non-match
		"user logged in ok", // for the regexp case
	}
	for i, body := range bodies {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "api")
		rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		rec.Body().SetStr(body)
		require.NoError(t, b.ConsumeLogs(ctx, ld))
		// Flush each record to its own immutable part, so part-level bloom pruning is exercised.
		require.NoError(t, store.Admin().Flush(ctx, "default", signal.Log))
	}

	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)
	params := logqlengine.EvalParams{Start: start, End: end, Step: time.Minute, Limit: 1000}
	run := func(t *testing.T, optimize bool, query string) []string {
		t.Helper()
		var opts logqlengine.Options
		if optimize {
			opts.Optimizers = []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}
		}
		engine, err := logqlengine.NewEngine(b.Logs(), opts)
		require.NoError(t, err)
		q, err := engine.NewQuery(ctx, query)
		require.NoError(t, err)
		data, err := q.Eval(ctx, params)
		require.NoError(t, err)
		return normalize(t, data)
	}

	for _, tt := range []struct {
		name, query string
		want        []string
	}{
		{
			name:  "multiword substring keeps glued match",
			query: `{service_name="api"} |= "GET /a 200"`,
			want:  []string{"line:GET /a 200", "line:xGET /a 200y"},
		},
		{
			name:  "regexp multiword literal prunes and keeps match",
			query: `{service_name="api"} |~ "user logged in"`,
			want:  []string{"line:user logged in ok"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := run(t, true, tt.query)
			require.Equal(t, run(t, false, tt.query), got, "optimized result must equal engine-only result")
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
