package storagebackend_test

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// TestLogRecordAttrPushdown checks that selecting by a clean record-attribute label (the Loki shape:
// stream labels arrive as per-record attributes with an empty resource) returns exactly the records
// matchSelector would, with the equality offloaded to a per-record fetch condition. It also checks
// that a dotted record-attribute label (http_method ← http.method) — which cannot be resolved to a
// raw key safely — still returns correct results via the in-memory fallback.
func TestLogRecordAttrPushdown(t *testing.T) {
	b, ctx := newBackend(t)
	ts := time.Now().Truncate(time.Second)

	type rec struct {
		job    string
		method string // stored under the dotted key http.method
		line   string
	}
	recs := []rec{
		{"varlogs", "GET", "a"},
		{"syslog", "POST", "b"},
		{"varlogs", "POST", "c"},
		{"applogs", "GET", "d"},
		{"varlogs", "GET", "e"},
	}
	for i, r := range recs {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty() // empty resource (Loki-style)
		sl := rl.ScopeLogs().AppendEmpty()
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		lr.Body().SetStr(r.line)
		lr.Attributes().PutStr("job", r.job)
		lr.Attributes().PutStr("http.method", r.method)
		require.NoError(t, b.ConsumeLogs(ctx, ld))
	}

	lq := b.Logs()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)
	query := func(sel []logql.LabelMatcher) []string {
		t.Helper()
		node, err := lq.Query(ctx, sel)
		require.NoError(t, err)
		it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
		require.NoError(t, err)
		var lines []string
		var e logqlengine.Entry
		for it.Next(&e) {
			lines = append(lines, e.Line)
		}
		require.NoError(t, it.Err())
		sort.Strings(lines)
		return lines
	}

	eq := func(label, value string) []logql.LabelMatcher {
		return []logql.LabelMatcher{{Label: logql.Label(label), Op: logql.OpEq, Value: value}}
	}

	// Clean record-attribute label: offloaded to a fetch condition.
	require.Equal(t, []string{"a", "c", "e"}, query(eq("job", "varlogs")))
	require.Equal(t, []string{"b"}, query(eq("job", "syslog")))
	require.Empty(t, query(eq("job", "none")))

	// Two clean record-attribute equalities (ANDed conditions).
	require.Equal(t, []string{"a", "e"}, query([]logql.LabelMatcher{
		{Label: "job", Op: logql.OpEq, Value: "varlogs"},
		{Label: "http_method", Op: logql.OpEq, Value: "GET"}, // dotted -> fallback, still correct
	}))

	// Dotted record-attribute label alone: not offloadable, correct via fallback.
	require.Equal(t, []string{"a", "d", "e"}, query(eq("http_method", "GET")))
}
