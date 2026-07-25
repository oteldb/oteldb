package storagebackend_test

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// traceqlPushdownQueries are the query shapes the pushdown must not change the answer of. They cover
// the operators it deliberately refuses to push (`!=`, `<`, `<=` and a nil comparison, all of which
// are true for a missing attribute), the unscoped attribute (which the engine resolves against span,
// scope and resource attributes at once), the intrinsics with and without a column form, the union
// shapes that need one fetch per branch, and a predicate matching nothing at all.
var traceqlPushdownQueries = []string{
	`{}`,
	`{name = "checkout.process"}`,
	`{name =~ "checkout.*"}`,
	`{name != "checkout.process"}`,
	`{statusMessage = "payment declined by upstream"}`,
	`{status = error}`,
	`{status != error}`,
	`{kind = server}`,
	`{kind != server}`,
	`{duration > 150ms}`,
	`{duration <= 20ms}`,
	`{span.http.route = "` + traceqlSelectedRoute + `"}`,
	`{span.http.route != "` + traceqlSelectedRoute + `"}`,
	`{span.http.route =~ "/route/1.*"}`,
	`{span.http.route !~ "` + traceqlSelectedRoute + `"}`,
	`{span.http.response.status_code = 500}`,
	`{span.http.response.status_code >= 500}`,
	`{span.http.response.status_code < 500}`,
	`{span.http.route != nil}`,
	`{span.http.route = nil}`,
	`{.http.route = "` + traceqlSelectedRoute + `"}`,
	`{.http.route != nil}`,
	// An unscoped attribute that lives on the resource, not the span: the span-attribute alternative
	// selects nothing and the stream-label one carries the query.
	`{.service.name = "payments"}`,
	`{.service.name = "nonexistent"}`,
	`{.http.route = "` + traceqlComboRoute + `" && status = error}`,
	`{span.nonexistent.attribute = "nope"}`,
	`{resource.service.name = "payments"}`,
	`{resource.service.name =~ "front.*"}`,
	`{resource.host.name = "host-cart"}`,
	`{instrumentation:name = "oteldb/goldenbench"}`,
	`{rootName = "` + traceqlRootName + `"}`,
	`{rootName = "nonexistent"}`,
	`{rootName =~ "GET.*"}`,
	`{rootServiceName = "` + traceqlRootService + `"}`,
	`{rootServiceName = "payments"}`,
	`{rootServiceName = ""}`,
	// A root intrinsic and a span-level predicate constrain different spans, so their candidate sets
	// intersect rather than sharing a fetch.
	`{rootName = "` + traceqlRootName + `" && status = error}`,
	`{rootServiceName = "` + traceqlRootService + `" && span.http.route = "` + traceqlSelectedRoute + `"}`,
	`{rootName = "` + traceqlRootName + `"} || {status = error}`,
	`{traceDuration > 1ms}`,
	`{span.http.request.method = "GET" && span.http.route = "` + traceqlComboRoute + `"}`,
	`{span.http.route = "` + traceqlSelectedRoute + `" || status = error}`,
	`{status = error} && {kind = server}`,
	// A scalar filter stage: its aggregate contributes a bare attribute matcher, which must not be
	// pushed as an existence filter (`by(...)`/`select(...)` stages are not supported by the engine
	// at all, so they cannot be covered here).
	`{status = error} | count() > 1`,
	`{span.http.route = "` + traceqlSelectedRoute + `"} | avg(duration) > 1ms`,
}

// TestTraceQLPushdownEquivalence asserts that lowering a query's span matchers to storage filters
// returns exactly the traces the plain full window scan does, over the golden corpus. Both engines
// read the same store, so the corpus is ingested once.
func TestTraceQLPushdownEquivalence(t *testing.T) {
	ctx := context.Background()

	pushed := traceqlNewFixture(t)
	plain := traceqlengine.NewEngine(
		storagebackend.New(pushed.store, storagebackend.WithTraceQLPushdown(false)).Traces(),
		traceqlengine.Options{},
	)

	// The id lookups are spelled from the corpus, so they cannot silently become always-false.
	traceID := traceqlTraceID(3)
	spanID := traceqlSpanID(3, 5)
	queries := append([]string(nil), traceqlPushdownQueries...)
	queries = append(queries,
		`{trace:id = "`+hex.EncodeToString(traceID[:])+`"}`,
		`{span:id = "`+hex.EncodeToString(spanID[:])+`"}`,
		`{span:parentID = "`+hex.EncodeToString(spanID[:])+`"}`,
	)

	for _, query := range append(queries, traceqlGoldenQueries()...) {
		t.Run(query, func(t *testing.T) {
			want, err := plain.Eval(ctx, query, pushed.evalParams())
			require.NoError(t, err)

			got, err := pushed.engine.Eval(ctx, query, pushed.evalParams())
			require.NoError(t, err)

			require.Equal(t, want, got)
		})
	}
}

// traceqlGoldenQueries returns the golden benchmark set's queries, so the equivalence test covers
// every shape the benchmarks measure (including the structural operators).
func traceqlGoldenQueries() []string {
	out := make([]string, 0, len(traceqlCases))
	for _, c := range traceqlCases {
		out = append(out, c.query)
	}
	return out
}

// TestTraceQLRootlessTraces covers a trace with no parentless span, which happens when the root was
// never ingested or starts outside the query window (the span fetch is windowed). Its root name and
// service are empty, as Tempo reports them, and the pushdown agrees with the plain scan on both the
// predicate that misses it and the one that selects it.
func TestTraceQLRootlessTraces(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)
	ts := time.Unix(1_600_000_000, 0).UTC()

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range 2 {
		// Every span's parent is a span that was never ingested, so the trace has no root.
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4}))
		s.SetSpanID(pcommon.SpanID([8]byte{byte(i) + 1}))
		s.SetParentSpanID(pcommon.SpanID([8]byte{0xff}))
		s.SetName("orphan")
		s.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
		s.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	}
	require.NoError(t, b.ConsumeTraces(ctx, td))

	var (
		params = traceqlengine.EvalParams{Start: ts.Add(-time.Hour), End: ts.Add(time.Hour), Limit: 10}
		plain  = storagebackend.New(store, storagebackend.WithTraceQLPushdown(false))
	)
	eval := func(t *testing.T, be *storagebackend.Backend, query string) *tempoapi.Traces {
		t.Helper()

		res, err := traceqlengine.NewEngine(be.Traces(), traceqlengine.Options{}).Eval(ctx, query, params)
		require.NoError(t, err)

		return res
	}

	tests := []struct {
		query string
		want  int
	}{
		// The trace has no root span, so it has no root name to match — the pushdown selects
		// parentless spans, and dropping the trace is what the engine does anyway.
		{query: `{rootName = "orphan"}`, want: 0},
		{query: `{rootServiceName = "api"}`, want: 0},
		// An empty root name is exactly what a rootless trace has. It is deliberately not pushed (the
		// filter could never select it), so this falls back to the plain scan.
		{query: `{rootName = ""}`, want: 1},
		{query: `{rootServiceName = ""}`, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := eval(t, b, tt.query)
			require.Len(t, got.Traces, tt.want)
			require.Equal(t, eval(t, plain, tt.query), got, "pushdown must agree with the plain scan")
		})
	}
}
