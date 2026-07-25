package storagebackend_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/storagebackend"
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
	`{.http.route = "` + traceqlSelectedRoute + `"}`,
	`{span.nonexistent.attribute = "nope"}`,
	`{resource.service.name = "payments"}`,
	`{resource.service.name =~ "front.*"}`,
	`{resource.host.name = "host-cart"}`,
	`{instrumentation:name = "oteldb/goldenbench"}`,
	`{rootName = "` + traceqlRootName + `"}`,
	`{rootServiceName = "` + traceqlRootService + `"}`,
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
