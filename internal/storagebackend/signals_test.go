package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func newBackend(t *testing.T) (*storagebackend.Backend, context.Context) {
	t.Helper()
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })
	return storagebackend.New(store), ctx
}

// TestBackendLogsRoundtrip ingests OTLP logs through the storage sink and queries them back through
// the LogQL querier adapter.
func TestBackendLogsRoundtrip(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	rec := sl.LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	rec.Body().SetStr("hello world")
	rec.SetSeverityNumber(plog.SeverityNumberInfo)

	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lq := b.Logs()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)

	// Pipeline query for the stream.
	node, err := lq.Query(ctx, []logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "api"}})
	require.NoError(t, err)
	it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
	require.NoError(t, err)
	entries := drain(t, it)
	require.Len(t, entries, 1)
	require.Equal(t, "hello world", entries[0].Line)

	// A non-matching selector yields nothing.
	node, err = lq.Query(ctx, []logql.LabelMatcher{{Label: "service_name", Op: logql.OpEq, Value: "other"}})
	require.NoError(t, err)
	it, err = node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
	require.NoError(t, err)
	require.Empty(t, drain(t, it))

	// LabelNames includes the stream label.
	names, err := lq.LabelNames(ctx, logstorage.LabelsOptions{Start: start, End: end})
	require.NoError(t, err)
	require.Contains(t, names, "service_name")

	// Series returns the stream label set.
	series, err := lq.Series(ctx, logstorage.SeriesOptions{Start: start, End: end})
	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Equal(t, "api", series[0]["service_name"])
}

// TestBackendTracesRoundtrip ingests OTLP spans through the storage sink and queries them back
// through the traces querier adapter.
func TestBackendTracesRoundtrip(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api")
	ss := rs.ScopeSpans().AppendEmpty()

	parent := ss.Spans().AppendEmpty()
	parent.SetTraceID(traceID)
	parent.SetSpanID(pcommon.SpanID([8]byte{1, 1, 1, 1, 1, 1, 1, 1}))
	parent.SetName("GET /")
	parent.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
	parent.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	parent.Attributes().PutStr("http.method", "GET")

	child := ss.Spans().AppendEmpty()
	child.SetTraceID(traceID)
	child.SetSpanID(pcommon.SpanID([8]byte{2, 2, 2, 2, 2, 2, 2, 2}))
	child.SetName("db.query")
	child.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
	child.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))

	require.NoError(t, b.ConsumeTraces(ctx, td))

	tq := b.Traces()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)

	// TraceByID returns both spans.
	var id [16]byte = traceID
	spans := drain(t, mustTraceByID(t, ctx, tq, id, start, end))
	require.Len(t, spans, 2)

	// SearchTags by span attribute returns the matching span.
	found := drain(t, mustSearchTags(t, ctx, tq, map[string]string{"http.method": "GET"}, start, end))
	require.Len(t, found, 1)
	require.Equal(t, "GET /", found[0].Name)

	// TagNames includes the span attribute.
	tagNames, err := tq.TagNames(ctx, tracestorage.TagNamesOptions{Start: start, End: end})
	require.NoError(t, err)
	var keys []string
	for _, tn := range tagNames {
		keys = append(keys, tn.Name)
	}
	require.Contains(t, keys, "http.method")
}

// TestBackendProfilesRoundtrip ingests an OTLP profile through the storage sink and queries the
// merged flamegraph back through the profiles querier adapter.
func TestBackendProfilesRoundtrip(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)

	pd := pprofile.NewProfiles()
	dict := pd.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds", "main")

	fn := dict.FunctionTable().AppendEmpty()
	fn.SetNameStrindex(3)
	mp := dict.MappingTable().AppendEmpty()
	mp.SetFilenameStrindex(3)
	loc := dict.LocationTable().AppendEmpty()
	loc.SetMappingIndex(0)
	line := loc.Lines().AppendEmpty()
	line.SetFunctionIndex(0)
	line.SetLine(42)
	stack := dict.StackTable().AppendEmpty()
	stack.LocationIndices().Append(0)

	rp := pd.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "api")
	scp := rp.ScopeProfiles().AppendEmpty()
	p := scp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	p.PeriodType().SetTypeStrindex(1)
	p.PeriodType().SetUnitStrindex(2)
	p.SetTime(pcommon.Timestamp(ts.UnixNano()))
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(123)
	s.TimestampsUnixNano().Append(uint64(ts.UnixNano()))

	require.NoError(t, b.ConsumeProfiles(ctx, pd))

	pq := b.Profiles()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)

	// ProfileTypes enumerates the ingested type.
	types, err := pq.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{Start: start, End: end})
	require.NoError(t, err)
	require.Len(t, types, 1)
	require.Equal(t, "cpu", types[0].SampleType)
	require.Equal(t, "nanoseconds", types[0].SampleUnit)

	// SelectMergeProfile resolves the stack and accumulates the sample value.
	tree, err := pq.SelectMergeProfile(ctx, profilestorage.SelectProfileParams{
		Type: profileql.ProfileType{
			Name:       "cpu",
			SampleType: "cpu",
			SampleUnit: "nanoseconds",
			PeriodType: "cpu",
			PeriodUnit: "nanoseconds",
		},
		Start: start,
		End:   end,
	})
	require.NoError(t, err)
	require.Equal(t, int64(123), tree.Total())
	require.NotNil(t, tree.Root)
	require.Len(t, tree.Root.Children, 1)
	require.Equal(t, "main", tree.Root.Children[0].Name)
	require.Equal(t, int64(123), tree.Root.Children[0].Self)
}

func drain[T any](t *testing.T, it iterators.Iterator[T]) []T {
	t.Helper()
	t.Cleanup(func() { _ = it.Close() })
	var out []T
	var v T
	for it.Next(&v) {
		out = append(out, v)
	}
	require.NoError(t, it.Err())
	return out
}

func mustTraceByID(t *testing.T, ctx context.Context, tq *storagebackend.TraceQuerier, id [16]byte, start, end time.Time) iterators.Iterator[tracestorage.Span] {
	t.Helper()
	it, err := tq.TraceByID(ctx, id, tracestorage.TraceByIDOptions{Start: start, End: end})
	require.NoError(t, err)
	return it
}

func mustSearchTags(t *testing.T, ctx context.Context, tq *storagebackend.TraceQuerier, tags map[string]string, start, end time.Time) iterators.Iterator[tracestorage.Span] {
	t.Helper()
	it, err := tq.SearchTags(ctx, tags, tracestorage.SearchTagsOptions{Start: start, End: end})
	require.NoError(t, err)
	return it
}
