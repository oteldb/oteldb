package storagebackend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
