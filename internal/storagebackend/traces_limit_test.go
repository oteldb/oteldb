package storagebackend_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// TestTraceQLLimitEquivalence is the safety net for bounding the candidate traces by the query's
// limit: for every query shape and every limit, the bounded engine must return exactly what the
// unbounded one does.
//
// The reference engine has the pushdown disabled, so it never bounds anything and materializes the
// whole window — the behavior the limit pushdown has to be invisible against. A limit below the
// corpus size is the point: it makes an over-eager bound (one applied to a candidate set that is a
// superset of the matches, or one that picks a different prefix) return a different set of traces,
// not merely a slower query.
func TestTraceQLLimitEquivalence(t *testing.T) {
	ctx := context.Background()

	bounded := traceqlNewFixture(t)
	plain := traceqlEngineOver(t, bounded, storagebackend.WithTraceQLPushdown(false))

	windows := []struct {
		name       string
		start, end time.Time
		// wholeTraces reports that every trace of the corpus is fully inside the window. A partial
		// window has to skip the multi-spanset queries: the engine's spanset operators index a[0]
		// and b[0] after checking only that *both* sides are empty, so a trace where the window cut
		// one side away panics — a pre-existing engine bug, unrelated to the limit.
		wholeTraces bool
	}{
		{name: "full", start: bounded.start, end: bounded.end, wholeTraces: true},
		// A window ending mid-corpus: every trace whose spans run past the end is dropped by the
		// engine after the querier returned it, so a bound that spends a slot on one comes up short.
		{
			name:  "truncated",
			start: bounded.start,
			end:   traceqlStart.Add(traceqlTraces / 2 * time.Millisecond),
		},
		// A window starting mid-corpus, so the first traces in scan order are gone.
		{
			name:  "late",
			start: traceqlStart.Add(traceqlTraces / 2 * time.Millisecond),
			end:   bounded.end,
		},
	}

	for _, w := range windows {
		t.Run(w.name, func(t *testing.T) {
			for _, limit := range []int{1, 2, 7, 20, 64, traceqlTraces - 1, traceqlTraces, traceqlTraces + 1} {
				t.Run("limit"+strconv.Itoa(limit), func(t *testing.T) {
					for _, query := range traceqlLimitQueries() {
						if !w.wholeTraces && strings.Count(query, "{") > 1 {
							continue
						}
						t.Run(query, func(t *testing.T) {
							params := traceqlengine.EvalParams{Start: w.start, End: w.end, Limit: limit}

							want, err := plain.Eval(ctx, query, params)
							require.NoError(t, err)

							got, err := bounded.engine.Eval(ctx, query, params)
							require.NoError(t, err)

							require.Equal(t, want, got)
						})
					}
				})
			}
		})
	}
}

// traceqlLimitQueries is the corpus of shapes the limit equivalence runs over: the pushdown's own
// set plus the golden benchmark queries, so both the exact shapes (which get bounded) and every
// refused one (which must not be) are covered.
func traceqlLimitQueries() []string {
	queries := append([]string(nil), traceqlPushdownQueries...)
	return append(queries, traceqlGoldenQueries()...)
}

// TestTraceQLLimitOverrunningTrace covers the one engine-side check the bound has to reproduce: a
// trace whose spans start inside the window but end after it is dropped by the engine, and a fetch
// cannot express that (it filters on the start timestamp alone). If the bound spent a slot on such
// a trace, a limited query would come up one result short.
//
// The golden corpus cannot exercise this — its overrunning traces are the last ones in scan order,
// so they never fall inside a limit's prefix — hence the purpose-built corpus here: the *earliest*
// trace is the one that overruns.
func TestTraceQLLimitOverrunningTrace(t *testing.T) {
	ctx := context.Background()

	start := time.Unix(1_700_000_000, 0).UTC()
	end := start.Add(time.Minute)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	// Enough traces that the limits below are still worth bounding (traceLimitBoundFactor).
	for i := range 12 {
		var id [16]byte
		id[15] = byte(i + 1)

		// The first trace outlives the window; the rest are well inside it.
		duration := time.Second
		if i == 0 {
			duration = 2 * time.Minute
		}

		s := spans.AppendEmpty()
		s.SetTraceID(id)
		s.SetSpanID(pcommon.SpanID([8]byte{byte(i + 1)}))
		s.SetName("op")
		spanStart := start.Add(time.Duration(i) * time.Second)
		s.SetStartTimestamp(pcommon.Timestamp(spanStart.UnixNano()))
		s.SetEndTimestamp(pcommon.Timestamp(spanStart.Add(duration).UnixNano()))
	}

	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	be := storagebackend.New(store)
	require.NoError(t, be.ConsumeTraces(ctx, td))
	require.NoError(t, store.Admin().Flush(ctx, traceqlTenant, signal.Trace))

	bounded := traceqlengine.NewEngine(be.Traces(), traceqlengine.Options{})
	plain := traceqlengine.NewEngine(
		storagebackend.New(store, storagebackend.WithTraceQLPushdown(false)).Traces(),
		traceqlengine.Options{},
	)

	for _, limit := range []int{1, 2, 3} {
		t.Run("limit"+strconv.Itoa(limit), func(t *testing.T) {
			params := traceqlengine.EvalParams{Start: start, End: end, Limit: limit}

			want, err := plain.Eval(ctx, `{name = "op"}`, params)
			require.NoError(t, err)
			require.Len(t, want.Traces, limit, "the overrunning trace must not count")

			got, err := bounded.Eval(ctx, `{name = "op"}`, params)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestTraceQLLimitBounds asserts the bound is actually taken where it should be, so the equivalence
// test above is not passing merely because nothing is ever bounded. It counts the spans the querier
// hands the engine: an exact query with a small limit must yield exactly that many traces, an
// inexact one every candidate.
func TestTraceQLLimitBounds(t *testing.T) {
	ctx := context.Background()
	f := traceqlNewFixture(t)

	const limit = 20
	countTraces := func(t *testing.T, query string) int {
		t.Helper()

		params := traceqlengine.SelectSpansetsParams{
			Start: f.start,
			End:   f.end,
			Limit: limit,
		}
		expr, err := traceql.Parse(query)
		require.NoError(t, err)
		params.Op, params.Matchers = traceql.ExtractMatchers(expr)
		params.Exact = traceql.IsExactSpansetFilter(expr)

		iter, err := f.querier.SelectSpansets(ctx, params)
		require.NoError(t, err)
		defer func() { require.NoError(t, iter.Close()) }()

		var (
			elem traceqlengine.Trace
			n    int
		)
		for iter.Next(&elem) {
			n++
		}
		require.NoError(t, iter.Err())

		return n
	}

	// `{name = ...}` matches every trace, so an unbounded querier would return all of them.
	require.Equal(t, limit, countTraces(t, `{name = "`+traceqlRootName+`"}`))
	// The stream matcher is not exact, so the whole candidate set is materialized.
	require.Equal(t, traceqlTraces, countTraces(t, `{resource.service.name = "payments"}`))
	// Fewer candidates than the limit: nothing to bound.
	require.Equal(t, traceqlRouteTraces, countTraces(t, `{span.http.route = "`+traceqlSelectedRoute+`"}`))
}

// TestTraceQLLimitCountsMatches pins the meaning of EvalParams.Limit: it caps the number of traces
// that *matched* the query, not the number of traces the storage layer looked at.
//
// The regression it guards is a limit applied in scan order inside SelectSpansets, before any
// matcher ran. The selective query below matches 8 of 500 traces, spread across the corpus, so a
// pre-filter truncation to 20 returned zero or one trace instead of all 8.
func TestTraceQLLimitCountsMatches(t *testing.T) {
	t.Parallel()

	var (
		ctx   = context.Background()
		f     = traceqlNewFixture(t)
		query = `{span.http.route = "` + traceqlSelectedRoute + `"}`
	)

	eval := func(limit int) int {
		params := f.evalParams()
		params.Limit = limit

		res, err := f.engine.Eval(ctx, query, params)
		require.NoError(t, err)

		return len(res.Traces)
	}

	// A limit above the match count returns every match, however deep into the scan they sit.
	for _, limit := range []int{traceqlRouteTraces, traceqlRouteTraces + 1, 20, traceqlTraces} {
		require.Equalf(t, traceqlRouteTraces, eval(limit), "limit %d", limit)
	}

	// Below it, the limit still truncates — but to that many matches.
	for _, limit := range []int{1, 3, traceqlRouteTraces - 1} {
		require.Equalf(t, limit, eval(limit), "limit %d", limit)
	}
}
