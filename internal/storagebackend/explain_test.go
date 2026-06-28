package storagebackend_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/query/profile"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// TestExplainAnalyzeLogs proves the storage EXPLAIN ANALYZE collector populates a
// profiled fetch tree when a query runs through oteldb's LogQL querier adapter —
// the same context plumbing the Explain HTTP middleware relies on.
func TestExplainAnalyzeLogs(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	rec.Body().SetStr("hello world")
	rec.SetSeverityNumber(plog.SeverityNumberInfo)
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	// Install the collector, exactly as the Explain middleware does per-request.
	qctx, coll := profile.WithCollector(ctx)

	lq := b.Logs()
	node, err := lq.Query(qctx, []logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "api"}})
	require.NoError(t, err)
	it, err := node.EvalPipeline(qctx, logqlengine.EvalParams{
		Start: ts.Add(-time.Hour), End: ts.Add(time.Hour), Limit: -1,
	})
	require.NoError(t, err)
	require.Len(t, drain(t, it), 1)

	root := coll.Root()
	require.NotNil(t, root)
	rendered := root.Render()
	t.Logf("EXPLAIN ANALYZE (logs):\n%s", rendered)
	require.Contains(t, rendered, "recordengine.fetch")
}

// TestExplainAnalyzeTraces does the same through the TraceQL querier adapter.
func TestExplainAnalyzeTraces(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(traceID)
	span.SetSpanID(pcommon.SpanID([8]byte{1, 1, 1, 1, 1, 1, 1, 1}))
	span.SetName("GET /")
	span.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
	span.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	span.Attributes().PutStr("http.method", "GET")
	require.NoError(t, b.ConsumeTraces(ctx, td))

	qctx, coll := profile.WithCollector(ctx)

	tq := b.Traces()
	it, err := tq.SearchTags(qctx, map[string]string{"http.method": "GET"},
		tracestorage.SearchTagsOptions{Start: ts.Add(-time.Hour), End: ts.Add(time.Hour)})
	require.NoError(t, err)
	require.Len(t, drain(t, it), 1)

	root := coll.Root()
	require.NotNil(t, root)
	rendered := root.Render()
	t.Logf("EXPLAIN ANALYZE (traces):\n%s", rendered)
	require.Contains(t, rendered, "recordengine.fetch")
}

// TestExplainAnalyzeDisabledIsNoop confirms the query path is unaffected when no
// collector is installed (the default, header-less request path).
func TestExplainAnalyzeDisabledIsNoop(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	rec.Body().SetStr("hello world")
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lq := b.Logs()
	node, err := lq.Query(ctx, []logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "api"}})
	require.NoError(t, err)
	it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{
		Start: ts.Add(-time.Hour), End: ts.Add(time.Hour), Limit: -1,
	})
	require.NoError(t, err)
	require.Len(t, drain(t, it), 1)

	// Sanity: the package's Render is stable and self-contained.
	require.NotContains(t, strings.TrimSpace((&profile.Node{Name: "query"}).Render()), "recordengine")
}
