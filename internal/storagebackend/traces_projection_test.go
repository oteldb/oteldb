package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type traceProjectionSource struct {
	storagebackend.Source
	full     bool
	requests []fetch.Request
}

func (s *traceProjectionSource) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return traceProjectionFetch{source: s, inner: s.Source.TraceFetcher(tenants...)}
}

type traceProjectionFetch struct {
	source *traceProjectionSource
	inner  fetch.Fetcher
}

func (f traceProjectionFetch) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	f.source.requests = append(f.source.requests, r)
	if f.source.full {
		r.Projection = nil
	}
	return f.inner.Fetch(ctx, r)
}

func projectionTraceFixture(tb testing.TB) (*storage.Storage, traceqlengine.EvalParams) {
	tb.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close(ctx)) })
	td := ptrace.NewTraces()
	base := time.Unix(1_700_000_000, 0).UTC()
	for _, service := range []string{"frontend", "payments"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", service)
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("projection-test")
		ss.Scope().SetVersion("v1")
		ss.Scope().Attributes().PutStr("scope.tag", "scope-value")
		for trace := range 2 {
			s := ss.Spans().AppendEmpty()
			s.SetTraceID(pcommon.TraceID{15: byte(trace + 1)})
			s.SetSpanID(pcommon.SpanID{7: 1})
			s.SetName("root")
			if service == "payments" {
				s.SetSpanID(pcommon.SpanID{7: 2})
				s.SetParentSpanID(pcommon.SpanID{7: 1})
				s.SetName("child")
			}
			start := base.Add(time.Duration(trace) * time.Second)
			s.SetStartTimestamp(pcommon.Timestamp(start.UnixNano()))
			s.SetEndTimestamp(pcommon.Timestamp(start.Add(10 * time.Millisecond).UnixNano()))
			s.SetKind(ptrace.SpanKindServer)
			s.Status().SetCode(ptrace.StatusCodeError)
			s.Status().SetMessage("failure")
			s.Attributes().PutInt("cost", 2)
			s.Attributes().PutStr("service.name", "span-service")
			s.Attributes().PutStr("tag", "span-value")
			e := s.Events().AppendEmpty()
			e.SetName("exception")
			e.SetTimestamp(pcommon.Timestamp(start.Add(time.Millisecond).UnixNano()))
			e.Attributes().PutStr("tag", "event-value")
			l := s.Links().AppendEmpty()
			l.SetTraceID(pcommon.TraceID{15: 3})
			l.SetSpanID(pcommon.SpanID{7: 4})
			l.Attributes().PutStr("tag", "link-value")
		}
	}
	require.NoError(tb, storagebackend.New(store).ConsumeTraces(ctx, td))
	return store, traceqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(2 * time.Second), Limit: 20}
}

func TestTraceQLProjectedFetch(t *testing.T) {
	t.Parallel()
	base := []string{sigtrace.ColTraceID, sigtrace.ColSpanID, sigtrace.ColParentSpanID, sigtrace.ColName, sigtrace.ColDuration}
	tests := []struct {
		name, query string
		columns     []string
	}{
		{name: "bare", query: `{}`},
		{name: "status", query: `{status = error}`, columns: []string{sigtrace.ColStatusCode}},
		{name: "kind", query: `{kind = server}`, columns: []string{sigtrace.ColKind}},
		{name: "message", query: `{statusMessage = "failure"}`, columns: []string{sigtrace.ColStatusMsg}},
		{name: "span_attribute", query: `{span.tag = "span-value"}`, columns: []string{sigtrace.ColAttrs}},
		{name: "resource_attribute", query: `{resource.service.name = "frontend"}`, columns: []string{sigtrace.ColAttrs}},
		{name: "scope", query: `{instrumentation:name = "projection-test"}`},
		{name: "scope_attribute", query: `{instrumentation.scope.tag = "scope-value"}`, columns: []string{sigtrace.ColAttrs}},
		{name: "event_name", query: `{event:name = "exception"}`, columns: []string{sigtrace.ColEvents}},
		{name: "event_time", query: `{event:timeSinceStart > 0s}`, columns: []string{sigtrace.ColEvents}},
		{name: "event_attribute", query: `{event.tag = "event-value"}`, columns: []string{sigtrace.ColEvents, sigtrace.ColAttrs}},
		{name: "link_id", query: `{link:spanID = "0000000000000004"}`, columns: []string{sigtrace.ColLinks}},
		{name: "link_attribute", query: `{link.tag = "link-value"}`, columns: []string{sigtrace.ColLinks, sigtrace.ColAttrs}},
		{name: "aggregate", query: `{} | avg(span.cost) > 1`, columns: []string{sigtrace.ColAttrs}},
		{name: "structural", query: `{name = "root"} >> {event.tag = "event-value"}`, columns: []string{sigtrace.ColEvents, sigtrace.ColAttrs}},
		{name: "binary", query: `{event:name = "exception"} && {link:spanID = "0000000000000004"}`, columns: []string{sigtrace.ColEvents, sigtrace.ColLinks}},
	}
	for _, flushed := range []bool{false, true} {
		name := "head"
		if flushed {
			name = "parts"
		}
		t.Run(name, func(t *testing.T) {
			store, params := projectionTraceFixture(t)
			if flushed {
				require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					source := &traceProjectionSource{Source: store}
					projected := traceqlengine.NewEngine(storagebackend.NewQuery(source).Traces(), traceqlengine.Options{})
					got, err := projected.Eval(t.Context(), tt.query, params)
					require.NoError(t, err)
					require.NotEmpty(t, got.Traces)
					source.full = true
					reference := traceqlengine.NewEngine(storagebackend.NewQuery(source).Traces(), traceqlengine.Options{})
					want, err := reference.Eval(t.Context(), tt.query, params)
					require.NoError(t, err)
					require.Equal(t, want, got)
					last := source.requests[len(source.requests)-1]
					require.ElementsMatch(t, append(append([]string(nil), base...), tt.columns...), last.Projection)
				})
			}
		})
	}
}

func BenchmarkTraceQLProjection(b *testing.B) {
	f := traceqlNewFixture(b, storagebackend.WithTraceQLPushdown(true))
	for _, query := range []string{`{}`, `{status = error}`, `{span.http.route = "/route/7"}`} {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(f.logical)
			for b.Loop() {
				_, err := f.engine.Eval(context.Background(), query, f.evalParams())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
