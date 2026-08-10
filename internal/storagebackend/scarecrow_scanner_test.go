package storagebackend_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	oteldbstorage "github.com/oteldb/storage"

	otelpromql "github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// ingestScannerFixture writes two series of "scan_metric" (foo="a" and foo="b") with distinct
// sample sequences, so matcher pushdown and per-series scanning can be told apart.
func ingestScannerFixture(ctx context.Context, t *testing.T, base time.Time) *storagebackend.Backend {
	t.Helper()

	store, err := oteldbstorage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("scan_metric")
	g := m.SetEmptyGauge()

	add := func(foo string, offsets []time.Duration, values []float64) {
		for i, off := range offsets {
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.Timestamp(base.Add(off).UnixNano()))
			dp.SetDoubleValue(values[i])
			dp.Attributes().PutStr("foo", foo)
		}
	}
	add("a", []time.Duration{-20 * time.Second, -10 * time.Second, 0}, []float64{1, 2, 3})
	add("b", []time.Duration{-15 * time.Second, -5 * time.Second}, []float64{10, 20})

	b := storagebackend.New(store)
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return b
}

// TestScarecrowScannerSeriesAndScan exercises Series and Scan directly: the union of both series
// with an unfiltered matcher, a single series with an equality matcher, and a negated matcher that
// cannot be pushed into the index (exercising the post-fetch re-check).
func TestScarecrowScannerSeriesAndScan(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(2_000_000, 0).UTC()

	b := ingestScannerFixture(ctx, t, base)
	s := b.ScarecrowScanner()
	t.Cleanup(func() { _ = s.Close() })

	mint := base.Add(-time.Minute).UnixMilli()
	maxt := base.UnixMilli()

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "scan_metric")

	t.Run("series union", func(t *testing.T) {
		lsets, err := s.Series(ctx, mint, maxt, []*labels.Matcher{nameMatcher})
		require.NoError(t, err)
		require.Len(t, lsets, 2)

		got := make([]string, len(lsets))
		for i, l := range lsets {
			got[i] = l.Get("foo")
		}
		slices.Sort(got)
		require.Equal(t, []string{"a", "b"}, got)
	})

	t.Run("equality matcher narrows to one series", func(t *testing.T) {
		it, err := s.Scan(ctx, mint, maxt, []*labels.Matcher{
			nameMatcher,
			labels.MustNewMatcher(labels.MatchEqual, "foo", "a"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = it.Close() })

		samples, err := it.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, samples)
		require.Equal(t, "a", samples.Labels.Get("foo"))
		require.Equal(t, []float64{1, 2, 3}, samples.V)
		require.Len(t, samples.T, 3)
		require.True(t, slices.IsSorted(samples.T), "timestamps must be ascending")

		next, err := it.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, next, "only one series matches")
	})

	t.Run("negated matcher falls back to post-fetch recheck", func(t *testing.T) {
		// foo!="a" cannot be pushed into the postings index (it matches the empty string too, i.e.
		// series lacking foo), so this exercises MatchesAll rather than PushableMatchers.
		lsets, err := s.Series(ctx, mint, maxt, []*labels.Matcher{
			nameMatcher,
			labels.MustNewMatcher(labels.MatchNotEqual, "foo", "a"),
		})
		require.NoError(t, err)
		require.Len(t, lsets, 1)
		require.Equal(t, "b", lsets[0].Get("foo"))
	})
}

// TestScarecrowScannerPushdowns exercises AggregateOverTime and CountSeries directly: matcher
// pushdown/recheck symmetry with Series/Scan, and the aggregate values themselves.
func TestScarecrowScannerPushdowns(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(2_000_000, 0).UTC()

	b := ingestScannerFixture(ctx, t, base)
	s := b.ScarecrowScanner().(interface {
		scarecrow.AggregateScanner
		scarecrow.SeriesCounter
		scarecrow.GroupedSeriesCounter
	})
	t.Cleanup(func() { _ = s.Close() })

	mint := base.Add(-time.Minute).UnixMilli()
	maxt := base.UnixMilli()

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "scan_metric")

	t.Run("aggregate over time sums per series", func(t *testing.T) {
		aggs, err := s.AggregateOverTime(ctx, mint, maxt, []*labels.Matcher{nameMatcher})
		require.NoError(t, err)
		require.Len(t, aggs, 2)

		byFoo := map[string]scarecrow.WindowAggregate{}
		for _, a := range aggs {
			byFoo[a.Labels.Get("foo")] = a
		}

		require.Equal(t, int64(3), byFoo["a"].Count)
		require.Equal(t, 6.0, byFoo["a"].Sum)
		require.Equal(t, 1.0, byFoo["a"].Min)
		require.Equal(t, 3.0, byFoo["a"].Max)

		require.Equal(t, int64(2), byFoo["b"].Count)
		require.Equal(t, 30.0, byFoo["b"].Sum)
	})

	t.Run("aggregate over time applies negated-matcher recheck", func(t *testing.T) {
		aggs, err := s.AggregateOverTime(ctx, mint, maxt, []*labels.Matcher{
			nameMatcher,
			labels.MustNewMatcher(labels.MatchNotEqual, "foo", "a"),
		})
		require.NoError(t, err)
		require.Len(t, aggs, 1)
		require.Equal(t, "b", aggs[0].Labels.Get("foo"))
	})

	t.Run("count series", func(t *testing.T) {
		n, err := s.CountSeries(ctx, mint, maxt, []*labels.Matcher{nameMatcher})
		require.NoError(t, err)
		require.Equal(t, uint64(2), n)
	})

	t.Run("count series applies negated-matcher recheck", func(t *testing.T) {
		n, err := s.CountSeries(ctx, mint, maxt, []*labels.Matcher{
			nameMatcher,
			labels.MustNewMatcher(labels.MatchNotEqual, "foo", "a"),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)
	})

	t.Run("count series by label", func(t *testing.T) {
		counts, err := s.CountSeriesBy(ctx, mint, maxt, "foo", []*labels.Matcher{nameMatcher})
		require.NoError(t, err)
		require.Equal(t, map[string]uint64{"a": 1, "b": 1}, counts)
	})

	t.Run("count series by label applies negated-matcher recheck", func(t *testing.T) {
		counts, err := s.CountSeriesBy(ctx, mint, maxt, "foo", []*labels.Matcher{
			nameMatcher,
			labels.MustNewMatcher(labels.MatchNotEqual, "foo", "a"),
		})
		require.NoError(t, err)
		require.Equal(t, map[string]uint64{"b": 1}, counts)
	})
}

// TestScarecrowScannerEngineMatchesFork runs a real query through internal/scarecrow wired to
// [storagebackend.Backend.ScarecrowScanner] and compares it against the same query answered by
// the production fork engine (internal/promql) over the same store — the differential oracle for
// the whole Scanner adapter (label projection, matcher pushdown, ms/ns conversion, release
// lifecycle), not just its two methods in isolation.
func TestScarecrowScannerEngineMatchesFork(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(2_000_000, 0).UTC()

	b := ingestScannerFixture(ctx, t, base)

	forkEng, err := otelpromql.New(b, promql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	scarecrowEng := scarecrow.NewEngine(scarecrow.Opts{
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return b.ScarecrowScanner() },
		Parser:     promqlparser.Options{},
	})

	queries := []string{
		`sum by (foo) (scan_metric)`,
		`sum_over_time(scan_metric[1m])`,
		`count(scan_metric)`,
		`count by (foo) (scan_metric)`,
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			forkQ, err := forkEng.NewInstantQuery(ctx, b, nil, query, base)
			require.NoError(t, err)
			t.Cleanup(forkQ.Close)
			forkRes := forkQ.Exec(ctx)
			require.NoError(t, forkRes.Err)
			forkVec, err := forkRes.Vector()
			require.NoError(t, err)

			scarecrowQ, err := scarecrowEng.NewInstantQuery(ctx, b, nil, query, base)
			require.NoError(t, err)
			t.Cleanup(scarecrowQ.Close)
			scarecrowRes := scarecrowQ.Exec(ctx)
			require.NoError(t, scarecrowRes.Err)
			scarecrowVec, err := scarecrowRes.Vector()
			require.NoError(t, err)

			sortVector(forkVec)
			sortVector(scarecrowVec)
			require.Equal(t, forkVec, scarecrowVec)
		})
	}
}

// TestScarecrowScannerGridMatchesFork is [TestScarecrowScannerEngineMatchesFork] over a stepped
// range, which is the only shape that reaches the [scarecrow.GridAggregateScanner] pushdown:
// gridFor declines a single-step grid, so every instant query above takes the per-window path and
// leaves AggregateGrid entirely untested.
//
// It is the oracle for the parts of AggregateGrid that no unit test can check on its own — the
// WindowSpec anchor (storage evaluates on the absolute grid without it, answering at timestamps
// the query never asked about), the half-open (t-width, t] boundary, and the request span having
// to reach back a full window before the first step.
//
// Several step sizes on purpose: a step that divides the range evenly and one that does not
// exercise different bucket-to-window slides inside storage.
func TestScarecrowScannerGridMatchesFork(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(2_000_000, 0).UTC()

	b := ingestScannerFixture(ctx, t, base)

	forkEng, err := otelpromql.New(b, promql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	scarecrowEng := scarecrow.NewEngine(scarecrow.Opts{
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return b.ScarecrowScanner() },
		Parser:     promqlparser.Options{},
	})

	queries := []string{
		`count(scan_metric)`,
		`count by (foo) (scan_metric)`,
		`sum_over_time(scan_metric[1m])`,
		`count_over_time(scan_metric[1m])`,
		`avg_over_time(scan_metric[1m])`,
		`min_over_time(scan_metric[1m])`,
		`max_over_time(scan_metric[1m])`,
		`present_over_time(scan_metric[1m])`,
		`sum by (foo) (scan_metric)`,
	}

	start := base.Add(-4 * time.Minute)

	for _, step := range []time.Duration{15 * time.Second, 30 * time.Second, 37 * time.Second} {
		for _, query := range queries {
			t.Run(query+" step="+step.String(), func(t *testing.T) {
				forkQ, err := forkEng.NewRangeQuery(ctx, b, nil, query, start, base, step)
				require.NoError(t, err)
				t.Cleanup(forkQ.Close)
				forkRes := forkQ.Exec(ctx)
				require.NoError(t, forkRes.Err)
				forkMx, err := forkRes.Matrix()
				require.NoError(t, err)

				scarecrowQ, err := scarecrowEng.NewRangeQuery(ctx, b, nil, query, start, base, step)
				require.NoError(t, err)
				t.Cleanup(scarecrowQ.Close)
				scarecrowRes := scarecrowQ.Exec(ctx)
				require.NoError(t, scarecrowRes.Err)
				scarecrowMx, err := scarecrowRes.Matrix()
				require.NoError(t, err)

				require.Equal(t, forkMx.String(), scarecrowMx.String())
			})
		}
	}
}
