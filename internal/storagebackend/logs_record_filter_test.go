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
	"github.com/oteldb/oteldb/internal/logstorage"
)

// TestLogRecordAttrPushdown checks that selecting by a record-attribute label (the Loki shape: stream
// labels arrive as per-record attributes with an empty resource) returns exactly the records
// matchSelector would, with the equality offloaded to a per-record fetch condition. Since the LogKeys
// rewire, a dotted record-attribute label (http_method ← http.method) resolves to its raw key
// (http.method) via the engine's key index and is also offloaded — still returning correct results.
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

	// Two record-attribute equalities (ANDed conditions); the dotted one resolves via LogKeys.
	require.Equal(t, []string{"a", "e"}, query([]logql.LabelMatcher{
		{Label: "job", Op: logql.OpEq, Value: "varlogs"},
		{Label: "http_method", Op: logql.OpEq, Value: "GET"}, // dotted -> resolved to http.method, offloaded
	}))

	// Dotted record-attribute label alone: offloaded via its raw key (http.method).
	require.Equal(t, []string{"a", "d", "e"}, query(eq("http_method", "GET")))
}

// TestLogRecordAttrLabelNames checks that LabelNames lists record-attribute keys (sourced from the
// engine's key index via LogKeys), normalized to LogQL labels, alongside resource/scope labels.
// Without LogKeys, stream enumeration alone would miss per-record attribute keys entirely.
func TestLogRecordAttrLabelNames(t *testing.T) {
	b, ctx := newBackend(t)
	ts := time.Now().Truncate(time.Second)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api") // resource (stream) label
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	lr.Body().SetStr("hello")
	lr.Attributes().PutStr("job", "varlogs")     // clean record-attribute key
	lr.Attributes().PutStr("http.method", "GET") // dotted record-attribute key -> http_method
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lq := b.Logs()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)
	names, err := lq.LabelNames(ctx, logstorage.LabelsOptions{Start: start, End: end})
	require.NoError(t, err)

	require.Contains(t, names, "service_name") // resource label
	require.Contains(t, names, "job")          // clean record-attribute key
	require.Contains(t, names, "http_method")  // dotted record-attribute key, normalized
	require.True(t, sort.StringsAreSorted(names), "label names must be sorted")
}
