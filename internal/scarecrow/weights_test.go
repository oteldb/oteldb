package scarecrow_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// weightedScanner serves one fixed series carrying lossy-sampling weights.
//
// The Prometheus storage interface has no weight channel, so the compliance corpus and the
// differential tests cannot exercise §3.5 at all. This stands in for the storagebackend scanner,
// which will carry fetch.Batch.ScaleFactors.
type weightedScanner struct {
	ls      labels.Labels
	t       []int64
	v       []float64
	weights []float64
}

var _ scarecrow.Scanner = (*weightedScanner)(nil)

func (s *weightedScanner) Close() error { return nil }

func (s *weightedScanner) Series(context.Context, int64, int64, []*labels.Matcher) ([]labels.Labels, error) {
	return []labels.Labels{s.ls}, nil
}

func (s *weightedScanner) Scan(context.Context, int64, int64, []*labels.Matcher) (scarecrow.SeriesIterator, error) {
	return &weightedIterator{s: s}, nil
}

type weightedIterator struct {
	s    *weightedScanner
	done bool
}

func (it *weightedIterator) Close() error { return nil }

func (it *weightedIterator) Next(context.Context) (*scarecrow.Samples, error) {
	if it.done {
		return nil, nil
	}
	it.done = true

	return &scarecrow.Samples{
		Labels:  it.s.ls,
		T:       it.s.t,
		V:       it.s.v,
		Weights: it.s.weights,
	}, nil
}

// evalWeighted runs an instant query at t=60s against a scanner serving the given weights.
func evalWeighted(t *testing.T, qs string, weights []float64) float64 {
	t.Helper()

	sc := &weightedScanner{
		ls: labels.FromStrings("__name__", "m"),
		// Three samples inside a 1m window ending at 60s.
		t:       []int64{20_000, 40_000, 60_000},
		v:       []float64{1, 2, 3},
		weights: weights,
	}

	e := scarecrow.NewEngine(scarecrow.Opts{
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return sc },
	})

	q, err := e.NewInstantQuery(context.Background(), nil, nil, qs, time.Unix(60, 0))
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)

	v, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, v, 1, "query %q", qs)

	return v[0].F
}

// TestSamplingWeightPolicy pins the per-function matrix in docs/promql-engine.md §3.5. Getting
// the weighted/unweighted split wrong silently skews results only under ingest overload, which
// is exactly when the data matters most, so each row is asserted explicitly.
func TestSamplingWeightPolicy(t *testing.T) {
	t.Parallel()

	// Every sample stands for 4 originals: the sampler kept 1 in 4.
	sampled := []float64{4, 4, 4}

	for _, tt := range []struct {
		query      string
		unweighted float64
		weighted   float64
		why        string
	}{
		{
			query: "count_over_time(m[1m])", unweighted: 3, weighted: 12,
			why: "each survivor stands for 4 samples",
		},
		{
			query: "sum_over_time(m[1m])", unweighted: 6, weighted: 24,
			why: "(1+2+3) scaled by 4",
		},
		{
			query: "avg_over_time(m[1m])", unweighted: 2, weighted: 2,
			why: "frequency-weighted mean is unchanged by a uniform weight",
		},
		{
			query: "min_over_time(m[1m])", unweighted: 1, weighted: 1,
			why: "an extreme of the kept subset ignores weights",
		},
		{
			query: "max_over_time(m[1m])", unweighted: 3, weighted: 3,
			why: "an extreme of the kept subset ignores weights",
		},
		{
			query: "last_over_time(m[1m])", unweighted: 3, weighted: 3,
			why: "a single value is not an aggregate",
		},
		{
			query: "present_over_time(m[1m])", unweighted: 1, weighted: 1,
			why: "existence carries no weight",
		},
		{
			query: "changes(m[1m])", unweighted: 2, weighted: 2,
			why: "undercount is inherent, not fixable by weighting",
		},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()

			require.InDelta(t, tt.unweighted, evalWeighted(t, tt.query, nil), 1e-9,
				"unweighted baseline")
			require.InDelta(t, tt.weighted, evalWeighted(t, tt.query, sampled), 1e-9, tt.why)
		})
	}
}

// TestSamplingWeightIgnoredByRate pins the trap in §3.5: on a cumulative counter the surviving
// samples still carry correct cumulative values, so rate/increase/delta must NOT scale by
// weight. Weighting them would inflate the most-used function in PromQL by the sampling factor.
func TestSamplingWeightIgnoredByRate(t *testing.T) {
	t.Parallel()

	sampled := []float64{4, 4, 4}

	for _, qs := range []string{
		"rate(m[1m])",
		"increase(m[1m])",
		"delta(m[1m])",
		"irate(m[1m])",
		"idelta(m[1m])",
	} {
		t.Run(qs, func(t *testing.T) {
			t.Parallel()

			require.InDelta(t,
				evalWeighted(t, qs, nil),
				evalWeighted(t, qs, sampled),
				1e-9,
				"cumulative counters are unbiased by subsampling; weighting inflates by the sample factor",
			)
		})
	}
}

// TestSamplingWeightNonUniform checks the weighted folds against a non-uniform weight vector,
// where a uniform-weight test would not distinguish a correct implementation from one that
// multiplies by a single scalar at the end.
func TestSamplingWeightNonUniform(t *testing.T) {
	t.Parallel()

	w := []float64{1, 2, 5} // values are 1, 2, 3

	require.InDelta(t, 8.0, evalWeighted(t, "count_over_time(m[1m])", w), 1e-9)
	require.InDelta(t, 1*1+2*2+3*5, evalWeighted(t, "sum_over_time(m[1m])", w), 1e-9)
	require.InDelta(t, 20.0/8.0, evalWeighted(t, "avg_over_time(m[1m])", w), 1e-9)
}
