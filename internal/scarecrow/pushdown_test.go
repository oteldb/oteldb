package scarecrow_test

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// pushdownScanner implements every pushdown capability on top of the plain [scarecrow.Scanner],
// by doing exactly what the engine would have done anyway.
//
// That is the point: a pushdown must be an optimization and never a semantic change, so the
// strongest test is to answer it *the slow way* behind the fast interface and require the engine
// to produce identical results either way. A real storage implementation answers these from a
// stats sidecar and an index; if its answers differ from this one, the difference is storage's
// bug and not the planner's, and it stays out of the engine's tests.
//
// It also counts its calls, which is how the tests tell a pushdown that fired from one that
// silently fell back — a rewrite that never triggers passes every value assertion.
type pushdownScanner struct {
	scarecrow.Scanner

	// Atomic because a [scarecrow.Scanner] must tolerate concurrent use: a binary operator runs
	// both of its subtrees at once, against the one scanner the query was built with.
	aggregates atomic.Int64
	counts     atomic.Int64
	groupCount atomic.Int64
}

var (
	_ scarecrow.AggregateScanner     = (*pushdownScanner)(nil)
	_ scarecrow.SeriesCounter        = (*pushdownScanner)(nil)
	_ scarecrow.GroupedSeriesCounter = (*pushdownScanner)(nil)
)

// window collects the samples in (mint, maxt] per series, the exact window PromQL folds over.
func (s *pushdownScanner) window(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (byKey map[string]*scarecrow.WindowAggregate, order []string, err error) {
	it, err := s.Scan(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = it.Close() }()

	byKey = map[string]*scarecrow.WindowAggregate{}

	for {
		smp, err := it.Next(ctx)
		if err != nil {
			return nil, nil, err
		}

		if smp == nil {
			break
		}

		key := smp.Labels.String()

		for i, t := range smp.T {
			if t <= mint || t > maxt {
				continue
			}

			// Staleness markers are dropped, as [matrixFold] does when folding raw samples.
			// A real implementation of this capability never sees one — that is a documented
			// precondition — but the corpus data carries them, and emulating storage faithfully
			// here means matching what the engine would have computed, not re-testing the
			// precondition.
			if value.IsStaleNaN(smp.V[i]) {
				continue
			}

			a, ok := byKey[key]
			if !ok {
				a = &scarecrow.WindowAggregate{
					Labels: smp.Labels.Copy(),
					Min:    math.Inf(1),
					Max:    math.Inf(-1),
				}
				byKey[key] = a
				order = append(order, key)
			}

			v := smp.V[i]
			a.Count++
			a.Sum += v
			a.Min = math.Min(a.Min, v)
			a.Max = math.Max(a.Max, v)
		}
	}

	return byKey, order, nil
}

func (s *pushdownScanner) AggregateOverTime(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]scarecrow.WindowAggregate, error) {
	s.aggregates.Add(1)

	byKey, order, err := s.window(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}

	out := make([]scarecrow.WindowAggregate, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}

	return out, nil
}

func (s *pushdownScanner) CountSeries(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (uint64, error) {
	s.counts.Add(1)

	byKey, _, err := s.window(ctx, mint, maxt, matchers)
	if err != nil {
		return 0, err
	}

	return uint64(len(byKey)), nil
}

func (s *pushdownScanner) CountSeriesBy(
	ctx context.Context, mint, maxt int64, label string, matchers []*labels.Matcher,
) (map[string]uint64, error) {
	s.groupCount.Add(1)

	byKey, _, err := s.window(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}

	out := map[string]uint64{}
	for _, a := range byKey {
		out[a.Labels.Get(label)]++
	}

	return out, nil
}

const pushdownData = `
load 30s
  http_requests{job="api", instance="0", code="200"} 0+10x20
  http_requests{job="api", instance="1", code="200"} 0+20x20
  http_requests{job="api", instance="0", code="500"} 0+1x20
  http_requests{job="web", instance="0", code="200"} 0+5x20
  gauge{job="api"}                                  1 2 3 4 5 6 7 8 9 10
  sparse{job="api"}                                 1 _ _ _ _ 2 _ _ _ 3
`

// pushdownQueries are the shapes the rules are expected to rewrite, plus the shapes they must
// decline. Both matter equally: a rule that fires where it should not is a wrong answer, and the
// differential assertion catches it either way.
var pushdownQueries = []string{
	`count_over_time(http_requests[1m])`,
	`sum_over_time(http_requests[2m])`,
	`min_over_time(http_requests[2m])`,
	`max_over_time(http_requests[2m])`,
	`avg_over_time(http_requests[2m])`,
	`present_over_time(http_requests[1m])`,
	`count_over_time(sparse[3m])`,
	`avg_over_time(sparse[5m])`,
	`sum_over_time(gauge[45s])`,
	`sum_over_time(http_requests{job="api"}[1m] offset 30s)`,
	`sum_over_time(http_requests[1m] @ 300)`,
	`sum(sum_over_time(http_requests[2m])) by (job)`,
	`sum_over_time(http_requests[2m]) / count_over_time(http_requests[2m])`,

	// Must not be rewritten: the fold needs raw samples.
	`rate(http_requests[2m])`,
	`stddev_over_time(gauge[2m])`,
	`last_over_time(gauge[2m])`,

	// Counting.
	`count(http_requests)`,
	`count(http_requests{job="api"})`,
	`count by (job) (http_requests)`,
	`count by (code) (http_requests)`,
	`count by (instance) (http_requests)`,
	`count(sparse)`,
	`count(nonexistent)`,
	`count by (job) (nonexistent)`,
	`count(gauge) + count(http_requests)`,

	// Must not be rewritten: not a bare selector, or not a shape the seam can answer.
	`count(rate(http_requests[2m]))`,
	`count without (job) (http_requests)`,
	`count by (job, code) (http_requests)`,
}

// TestPushdownsPreserveResults is the property that makes a pushdown safe: the same query over
// the same data must produce the same answer whether or not the scanner offers the capability.
func TestPushdownsPreserveResults(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	plain := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true})

	var scanner *pushdownScanner
	pushed := scarecrow.NewEngine(scarecrow.Opts{
		EnableAtModifier: true,
		NewScanner: func(q storage.Queryable) scarecrow.Scanner {
			scanner = &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}

			return scanner
		},
	})

	for _, qs := range pushdownQueries {
		t.Run(qs, func(t *testing.T) {
			for _, ts := range []int64{60, 300, 600} {
				at := time.Unix(ts, 0)
				want := execInstant(t, plain, store, qs, at)
				got := execInstant(t, pushed, store, qs, at)
				require.Equalf(t, want, got, "instant at %ds", ts)
			}

			var (
				start = time.Unix(0, 0)
				end   = time.Unix(600, 0)
			)
			want := execRange(t, plain, store, qs, start, end, time.Minute)
			got := execRange(t, pushed, store, qs, start, end, time.Minute)
			require.Equal(t, want, got, "range 0..600 step 60s")
		})
	}
}

// TestPushdownsActuallyFire guards against the failure mode the differential test cannot see: a
// rule that never triggers agrees with the slow path perfectly.
func TestPushdownsActuallyFire(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	for _, tt := range []struct {
		query string
		// want reports which counter must be non-zero after the query runs.
		want func(s *pushdownScanner) int64
	}{
		{`sum_over_time(http_requests[2m])`, func(s *pushdownScanner) int64 { return s.aggregates.Load() }},
		{`count_over_time(http_requests[2m])`, func(s *pushdownScanner) int64 { return s.aggregates.Load() }},
		{`avg_over_time(http_requests[2m])`, func(s *pushdownScanner) int64 { return s.aggregates.Load() }},
		{`count(http_requests)`, func(s *pushdownScanner) int64 { return s.counts.Load() }},
		{`count by (job) (http_requests)`, func(s *pushdownScanner) int64 { return s.groupCount.Load() }},
	} {
		t.Run(tt.query, func(t *testing.T) {
			var scanner *pushdownScanner

			e := scarecrow.NewEngine(scarecrow.Opts{
				NewScanner: func(q storage.Queryable) scarecrow.Scanner {
					scanner = &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}

					return scanner
				},
			})

			execInstant(t, e, store, tt.query, time.Unix(300, 0))

			require.NotNil(t, scanner)
			require.Positive(t, tt.want(scanner), "pushdown did not fire")
		})
	}
}

// TestPushdownsDeclineUnsupportedShapes pins the rules' negative space: these must plan as they
// would with no capability at all, reading raw samples.
func TestPushdownsDeclineUnsupportedShapes(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	for _, query := range []string{
		`rate(http_requests[2m])`,
		`stddev_over_time(gauge[2m])`,
		`last_over_time(gauge[2m])`,
		`count(rate(http_requests[2m]))`,
		`count without (job) (http_requests)`,
		`count by (job, code) (http_requests)`,
	} {
		t.Run(query, func(t *testing.T) {
			var scanner *pushdownScanner

			e := scarecrow.NewEngine(scarecrow.Opts{
				NewScanner: func(q storage.Queryable) scarecrow.Scanner {
					scanner = &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}

					return scanner
				},
			})

			execInstant(t, e, store, query, time.Unix(300, 0))

			require.NotNil(t, scanner)
			require.Zero(t, scanner.aggregates.Load(), "aggregate pushdown must not fire")
			require.Zero(t, scanner.counts.Load(), "count pushdown must not fire")
			require.Zero(t, scanner.groupCount.Load(), "grouped count pushdown must not fire")
		})
	}
}

// TestPushdownsPreserveCompliance is the strongest statement available: the entire upstream
// corpus, run with every capability present, passes exactly the files it passes without them.
//
// The unit tests above cover the shapes the rules were written for; this covers the shapes
// nobody thought of, which is where a planner rewrite actually goes wrong.
func TestPushdownsPreserveCompliance(t *testing.T) {
	t.Parallel()

	opts := complianceOpts()
	opts.NewScanner = func(q storage.Queryable) scarecrow.Scanner {
		return &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
	}

	sb := &scoreboard{T: t}
	promqltest.RunBuiltinTests(sb, scarecrow.NewEngine(opts))

	require.Equal(t, compliancePassing, len(sb.passed),
		"pushdowns changed the corpus pass count; they must be transparent")
}
