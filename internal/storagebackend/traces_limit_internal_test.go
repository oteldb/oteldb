package storagebackend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// TestTracePushdownExact pins which lowerings are exact — the candidate traces are the traces
// holding a span the engine keeps, not a superset. Only an exact pushdown may bound the traces
// [TraceQuerier.SelectSpansets] materializes to the query's limit, so a wrong true here reproduces
// the under-reporting the plain scan had.
func TestTracePushdownExact(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		// Per-span conditions mirror the engine's own evaluater over a column that is always there.
		{query: `{name = "checkout.process"}`, want: true},
		{query: `{name != "checkout.process"}`, want: true},
		{query: `{name =~ "checkout.*"}`, want: true},
		{query: `{status = error}`, want: true},
		{query: `{kind = server}`, want: true},
		{query: `{duration > 150ms}`, want: true},
		{query: `{statusMessage = "declined"}`, want: true},
		{query: `{span:id = "0011223344556677"}`, want: true},
		{query: `{span:parentID = "0011223344556677"}`, want: true},
		{query: `{trace:id = "00112233445566770011223344556677"}`, want: true},
		{query: `{span.http.route = "/route/7"}`, want: true},
		{query: `{span.http.route =~ "/route/.*"}`, want: true},
		{query: `{status = error && span.http.route = "/route/7"}`, want: true},

		// A stream matcher decides whole streams on the resource *and* scope labels, while the
		// engine reads one of the two per span.
		{query: `{resource.service.name = "payments"}`, want: false},
		{query: `{instrumentation.library = "x"}`, want: false},
		{query: `{resource.service.name = "payments" && status = error}`, want: false},

		// A matcher left to the engine widens the candidate set.
		{query: `{status = error && span.http.route != "/route/7"}`, want: false},
		{query: `{status = error && rootName = "GET /"}`, want: false},
		{query: `{status = error && .http.route = "/route/7"}`, want: false},
		{query: `{status = error && name =~ "checkout(.*"}`, want: false},

		// A union is never exact: the caller may not bound it branch by branch.
		{query: `{span.http.route = "/route/7"} && {status = error}`, want: false},
		{query: `{name = "a"} >> {name = "b"}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)

			pd, ok := buildTracePushdown(traceql.ExtractMatchers(expr))
			require.True(t, ok, "nothing pushed")
			require.Equal(t, tt.want, pd.exact)
		})
	}
}

// TestLimitAppliesFailsClosed covers the gate itself: every precondition must hold, so dropping any
// one of them refuses the bound. The zero value of everything must refuse it too.
func TestLimitAppliesFailsClosed(t *testing.T) {
	var (
		q    = TraceQuerier{b: &Backend{traceQLPushdown: true, traceQLLimitPushdown: true}}
		pd   = tracePushdown{pushed: true, exact: true}
		base = traceqlengine.SelectSpansetsParams{Exact: true, Limit: 20}
	)
	const candidates = 20 * traceLimitBoundFactor
	require.True(t, q.limitApplies(base, pd, candidates))

	t.Run("Disabled", func(t *testing.T) {
		off := TraceQuerier{b: &Backend{traceQLPushdown: true}}
		require.False(t, off.limitApplies(base, pd, candidates))
	})

	t.Run("NotPushed", func(t *testing.T) {
		require.False(t, q.limitApplies(base, tracePushdown{exact: true}, candidates))
	})
	t.Run("InexactPushdown", func(t *testing.T) {
		require.False(t, q.limitApplies(base, tracePushdown{pushed: true}, candidates))
	})
	t.Run("InexactQuery", func(t *testing.T) {
		params := base
		params.Exact = false
		require.False(t, q.limitApplies(params, pd, candidates))
	})
	t.Run("NoLimit", func(t *testing.T) {
		for _, limit := range []int{0, -1} {
			params := base
			params.Limit = limit
			require.False(t, q.limitApplies(params, pd, candidates))
		}
	})
	t.Run("CandidatesNotWorthBounding", func(t *testing.T) {
		require.False(t, q.limitApplies(base, pd, candidates-1))
	})
	t.Run("ZeroValues", func(t *testing.T) {
		require.False(t, q.limitApplies(traceqlengine.SelectSpansetsParams{}, tracePushdown{}, 100))
	})
}

// TestTraceWithin covers the reproduction of the engine's per-trace window check, including the
// case a fetch cannot express: a span that starts inside the window and ends after it.
func TestTraceWithin(t *testing.T) {
	var (
		from = time.Unix(100, 0)
		to   = time.Unix(200, 0)
	)
	tests := []struct {
		name       string
		start, end int64
		from, to   time.Time
		want       bool
	}{
		{name: "Inside", start: from.UnixNano(), end: to.UnixNano(), from: from, to: to, want: true},
		{name: "EndsAfter", start: from.UnixNano(), end: to.UnixNano() + 1, from: from, to: to},
		{name: "StartsBefore", start: from.UnixNano() - 1, end: to.UnixNano(), from: from, to: to},
		{name: "Unbounded", start: 0, end: 1 << 60, want: true},
		{
			name: "OnlyStartBound", start: from.UnixNano(), end: to.UnixNano() + 1,
			from: from, want: true,
		},
		{
			name: "OnlyEndBound", start: from.UnixNano() - 1, end: to.UnixNano(),
			to: to, want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, traceWithin(tt.start, tt.end, tt.from, tt.to))
		})
	}
}
