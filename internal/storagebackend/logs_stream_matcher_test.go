package storagebackend_test

import (
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
