package storagebackend_test

import (
	"encoding/base64"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// TestLogStreamMatcherPushdown checks that offloading resource-label equality matchers to the
// postings index returns exactly the same records as in-memory filtering, across equality, no-match,
// negation, regexp, and the unfiltered case, over several resource streams.
func TestLogStreamMatcherPushdown(t *testing.T) {
	b, ctx := newBackend(t)
	ts := time.Now().Truncate(time.Second)

	for i, svc := range []string{"alpha", "beta", "gamma"} {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", svc)
		sl := rl.ScopeLogs().AppendEmpty()
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Second).UnixNano()))
		rec.Body().SetStr("log from " + svc)
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

	// Equality on a resource label is offloaded to the postings index.
	require.Equal(t, []string{"log from beta"},
		query([]logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "beta"}}))
	// No matching stream.
	require.Empty(t,
		query([]logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "nope"}}))
	// Negation is not offloaded but still correct via matchSelector.
	require.Equal(t, []string{"log from alpha", "log from gamma"},
		query([]logql.LabelMatcher{{Label: "service_name", Op: logql.OpNotEq, Value: "beta"}}))
	// Regexp is not offloaded but still correct.
	require.Equal(t, []string{"log from alpha", "log from beta"},
		query([]logql.LabelMatcher{{
			Label: "service_name", Op: logql.OpRe, Value: "alpha|beta",
			Re: regexp.MustCompile("^(?:alpha|beta)$"),
		}}))
	// Unfiltered returns every stream.
	require.Len(t, query(nil), 3)
}

// TestLogStreamMatcherPushdownBytesValue selects a stream by a bytes-valued resource attribute. The
// label set renders bytes as base64 (pcommon AsString), so the pushed matcher must too; a naive
// signal.Value.AppendText (raw bytes) would never equal the base64 query value and would silently
// prune both streams.
func TestLogStreamMatcherPushdownBytesValue(t *testing.T) {
	b, ctx := newBackend(t)
	ts := time.Now().Truncate(time.Second)

	tokens := map[string][]byte{"one": {0x01, 0x02, 0xff}, "two": {0x10, 0x20, 0x30}}
	for name, tok := range tokens {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutEmptyBytes("token").FromRaw(tok)
		sl := rl.ScopeLogs().AppendEmpty()
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		rec.Body().SetStr("log " + name)
		require.NoError(t, b.ConsumeLogs(ctx, ld))
	}

	// The query value is the base64 projection of token "one".
	wantB64 := base64.StdEncoding.EncodeToString(tokens["one"])

	lq := b.Logs()
	node, err := lq.Query(ctx, []logql.LabelMatcher{{Label: "token", Op: logql.OpEq, Value: wantB64}})
	require.NoError(t, err)
	it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: ts.Add(-time.Hour), End: ts.Add(time.Hour), Limit: -1})
	require.NoError(t, err)
	var lines []string
	var e logqlengine.Entry
	for it.Next(&e) {
		lines = append(lines, e.Line)
	}
	require.NoError(t, it.Err())
	require.Equal(t, []string{"log one"}, lines)
}
