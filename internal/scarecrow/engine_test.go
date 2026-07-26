package scarecrow_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func mustMatchers(t *testing.T, selector string) []*labels.Matcher {
	t.Helper()

	m, err := parser.NewParser(parser.Options{}).ParseMetricSelector(selector)
	require.NoError(t, err)

	return m
}

// emptyStorage is a queryable with no data, for expressions that touch no series.
func emptyStorage(t *testing.T) *promqltest.LazyLoader {
	t.Helper()

	ll, err := promqltest.NewLazyLoader("load 1m\n", promqltest.LazyLoaderOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ll.Close()) })

	return ll
}

func TestEngineInstantNumberLiteral(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})
	ts := time.Unix(60, 0)

	q, err := e.NewInstantQuery(context.Background(), emptyStorage(t).Queryable(), nil, "42", ts)
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)

	s, err := res.Scalar()
	require.NoError(t, err)
	require.InDelta(t, 42.0, s.V, 0)
	require.Equal(t, ts.UnixMilli(), s.T)
}

func TestEngineRangeNumberLiteral(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})
	start, end := time.Unix(0, 0), time.Unix(120, 0)

	q, err := e.NewRangeQuery(
		context.Background(), emptyStorage(t).Queryable(), nil, "7", start, end, 30*time.Second,
	)
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)

	m, err := res.Matrix()
	require.NoError(t, err)
	require.Len(t, m, 1)

	// A scalar is constant across every step of the grid.
	want := []promql.FPoint{
		{T: 0, F: 7}, {T: 30_000, F: 7}, {T: 60_000, F: 7}, {T: 90_000, F: 7}, {T: 120_000, F: 7},
	}
	require.Equal(t, want, m[0].Floats)
}

func TestEngineParenAndConstantsPlan(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	q, err := e.NewInstantQuery(
		context.Background(), emptyStorage(t).Queryable(), nil, "((3))", time.Unix(0, 0),
	)
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)

	s, err := res.Scalar()
	require.NoError(t, err)
	require.InDelta(t, 3.0, s.V, 0)
}

// TestEngineUnsupportedIsExplicit pins the current planning boundary: anything not yet planned
// must fail loudly rather than return a plausible-looking wrong answer.
//
// Entries leave this list as milestones land — it failing because a query started working is
// the intended signal, not a regression.
func TestEngineUnsupportedIsExplicit(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	for _, qs := range []string{
		`topk(3, up)`,                 // needs the full per-step series set
		`quantile(0.9, up)`,           // ditto
		`count_values("v", up)`,       // output schema is data-dependent
		`histogram_quantile(0.9, up)`, // M6: histograms
		`sort(up)`,                    // needs the full result to order it
	} {
		t.Run(qs, func(t *testing.T) {
			t.Parallel()

			q, err := e.NewInstantQuery(
				context.Background(), emptyStorage(t).Queryable(), nil, qs, time.Unix(0, 0),
			)
			require.NoError(t, err, "parsing must succeed; only planning is incomplete")

			defer q.Close()

			res := q.Exec(context.Background())
			require.ErrorIs(t, res.Err, scarecrow.ErrUnsupported)
		})
	}
}

// TestEngineParseErrorsSurface confirms the upstream parser's diagnostics reach the caller
// unmodified, which is the reason for consuming it rather than writing our own.
func TestEngineParseErrorsSurface(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	_, err := e.NewInstantQuery(
		context.Background(), emptyStorage(t).Queryable(), nil, "sum(", time.Unix(0, 0),
	)
	require.Error(t, err)
	// Position and wording come straight from the upstream parser, unwrapped.
	require.Contains(t, err.Error(), "1:5: parse error: unclosed left parenthesis")
}

func TestEngineRejectsNonVectorRangeQuery(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	_, err := e.NewRangeQuery(
		context.Background(), emptyStorage(t).Queryable(), nil,
		`"foo"`, time.Unix(0, 0), time.Unix(60, 0), time.Second,
	)
	require.ErrorContains(t, err, "invalid expression type")
}

func TestEngineRejectsDisabledModifiers(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	_, err := e.NewInstantQuery(
		context.Background(), emptyStorage(t).Queryable(), nil, `up @ 100`, time.Unix(0, 0),
	)
	require.ErrorContains(t, err, "@ modifier is disabled")

	_, err = e.NewInstantQuery(
		context.Background(), emptyStorage(t).Queryable(), nil, `up offset -5m`, time.Unix(0, 0),
	)
	require.ErrorContains(t, err, "negative offset is disabled")
}
