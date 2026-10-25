package tempoe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func TestStorageBackendTraceQLProjection(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	backend := storagebackend.New(store)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "projection-test")
	ss := rs.ScopeSpans().AppendEmpty()
	base := time.Unix(1_700_000_000, 0).UTC()
	var memory traceqlengine.MemoryQuerier
	for i := range 2 {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{15: 1})
		s.SetSpanID(pcommon.SpanID{7: byte(i + 1)})
		s.SetName("root")
		if i > 0 {
			s.SetParentSpanID(pcommon.SpanID{7: 1})
			s.SetName("child")
		}
		s.SetStartTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		s.SetEndTimestamp(pcommon.Timestamp(base.Add(10 * time.Millisecond).UnixNano()))
		s.Status().SetCode(ptrace.StatusCodeError)
		s.Attributes().PutStr("tag", "span-value")
		e := s.Events().AppendEmpty()
		e.SetName("exception")
		e.SetTimestamp(s.StartTimestamp() + pcommon.Timestamp(time.Millisecond))
		e.Attributes().PutStr("tag", "event-value")
		link := s.Links().AppendEmpty()
		link.SetTraceID(pcommon.TraceID{15: 3})
		link.SetSpanID(pcommon.SpanID{7: 4})
		link.Attributes().PutStr("tag", "link-value")
		memory.Add(tracestorage.NewSpanFromOTEL(uuid.Nil, rs.Resource(), ss.Scope(), s))
	}
	require.NoError(t, backend.ConsumeTraces(ctx, td))
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Trace))
	querier := backend.Traces()
	client := setupDB(t, integration.TraceProvider(t), querier, querier)
	reference := traceqlengine.NewEngine(&memory, traceqlengine.Options{})
	for _, query := range []string{
		`{}`,
		`{status = error}`,
		`{event:name = "exception"}`,
		`{event.tag = "event-value"}`,
		`{link:spanID = "0000000000000004"}`,
		`{link.tag = "link-value"}`,
		`{name = "root"} >> {name = "child"}`,
	} {
		t.Run(query, func(t *testing.T) {
			got, err := client.Search(ctx, tempoapi.SearchParams{
				Q: tempoapi.NewOptString(query), Limit: tempoapi.NewOptInt(1),
			})
			require.NoError(t, err)
			want, err := reference.Eval(ctx, query, traceqlengine.EvalParams{Limit: 1})
			require.NoError(t, err)
			require.NotEmpty(t, want.Traces)
			normalizeTraceSearchTimes(want.Traces)
			normalizeTraceSearchTimes(got.Traces)
			require.Equal(t, want.Traces, got.Traces)
		})
	}
}

func normalizeTraceSearchTimes(traces []tempoapi.TraceSearchMetadata) {
	for i := range traces {
		tr := &traces[i]
		tr.StartTimeUnixNano = tr.StartTimeUnixNano.UTC()
		for j := range tr.SpanSet.Value.Spans {
			s := &tr.SpanSet.Value.Spans[j]
			s.StartTimeUnixNano = s.StartTimeUnixNano.UTC()
		}
		for j := range tr.SpanSets {
			for k := range tr.SpanSets[j].Spans {
				s := &tr.SpanSets[j].Spans[k]
				s.StartTimeUnixNano = s.StartTimeUnixNano.UTC()
			}
		}
	}
}
