package storagebackend_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// TestBackendLogRecordAttrSelector verifies that a log stream can be selected by a per-record
// attribute even when it carries no resource or scope identity at all. This mirrors logs ingested
// over the Loki protocol (the OTel loki receiver maps Loki stream labels to log record attributes
// with no resource attributes), which previously were dropped because a label-less stream was not
// registered in the storage engine's postings all-set.
func TestBackendLogRecordAttrSelector(t *testing.T) {
	b, ctx := newBackend(t)
	ts := time.Now().Truncate(time.Second)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty() // no resource attributes
	sl := rl.ScopeLogs().AppendEmpty()    // no scope identity
	rec := sl.LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	rec.Body().SetStr("hello")
	rec.Attributes().PutStr("job", "varlogs")

	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lq := b.Logs()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)

	node, err := lq.Query(ctx, []logql.LabelMatcher{{Label: "job", Op: logql.OpEq, Value: "varlogs"}})
	require.NoError(t, err)
	it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
	require.NoError(t, err)
	entries := drain(t, it)
	require.Len(t, entries, 1, "record-attribute selector must match a stream with no resource identity")
	require.Equal(t, "hello", entries[0].Line)

	// A non-matching value yields nothing.
	node, err = lq.Query(ctx, []logql.LabelMatcher{{Label: "job", Op: logql.OpEq, Value: "other"}})
	require.NoError(t, err)
	it, err = node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
	require.NoError(t, err)
	require.Empty(t, drain(t, it))
}
