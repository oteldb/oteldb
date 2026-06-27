package storagebackend_test

import (
	"context"
	"math"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	promqlengine "github.com/oteldb/promql-engine/engine"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"

	otelpromql "github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// ingestGauge writes a gauge metric "ot_gauge" with one series (foo="bar") carrying the given
// (offset-from-evalT, value) samples, returning the populated store.
func ingestGauge(ctx context.Context, t *testing.T, evalT time.Time, samples []timeSample) *storage.Storage {
	t.Helper()

	store, err := storage.InMemory()
	require.NoError(t, err)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("ot_gauge")
	g := m.SetEmptyGauge()
	for _, s := range samples {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(evalT.Add(s.offset).UnixNano()))
		dp.SetDoubleValue(s.value)
		dp.Attributes().PutStr("foo", "bar")
	}

	b := storagebackend.New(store)
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return store
}

type timeSample struct {
	offset time.Duration
	value  float64
}

// vecLabels is the sortable label set of a PromQL sample (minus the empty __name__ a range-vector
// function drops), for deterministic comparison.
func sortVector(vec promql.Vector) {
	slices.SortFunc(vec, func(a, b promql.Sample) int {
		return labels.Compare(a.Metric, b.Metric)
	})
}

// TestOverTimePushdownMatchesRaw is the differential oracle for the aggregate pushdown: for every
// supported *_over_time function, the instant query answered from the aggregate sidecar
// (WithOverTimePushdown(true), the default) must equal the raw matrix-selector fold
// (WithOverTimePushdown(false)), including at the window boundary where a sample sits exactly on
// mint (PromQL's exclusive lower bound).
func TestOverTimePushdownMatchesRaw(t *testing.T) {
	ctx := context.Background()
	evalT := time.Unix(2_000_000, 0).UTC()

	// Samples at -30s, -20s, -10s from evalT. With a 30s range the window is (evalT-30s, evalT],
	// so the -30s sample is excluded; with a 40s range all three are in.
	store := ingestGauge(ctx, t, evalT, []timeSample{
		{-30 * time.Second, 10},
		{-20 * time.Second, 20},
		{-10 * time.Second, 30},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	pushdown := storagebackend.New(store)                                        // default: pushdown on
	raw := storagebackend.New(store, storagebackend.WithOverTimePushdown(false)) // raw fold

	engPushdown, err := otelpromql.New(pushdown, otelpromql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)
	engRaw, err := otelpromql.New(raw, otelpromql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"Count", "count_over_time(ot_gauge[30s])"},
		{"CountFull", "count_over_time(ot_gauge[40s])"},
		{"Sum", "sum_over_time(ot_gauge[30s])"},
		{"Min", "min_over_time(ot_gauge[30s])"},
		{"Max", "max_over_time(ot_gauge[30s])"},
		{"Avg", "avg_over_time(ot_gauge[30s])"},
		{"Present", "present_over_time(ot_gauge[30s])"},
		{"Filtered", `count_over_time(ot_gauge{foo="bar"}[30s])`},
		{"NegatedFilter", `count_over_time(ot_gauge{foo!="x"}[30s])`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)

			got := execInstant(ctx, t, engPushdown, pushdown, tt.query, evalT)
			want := execInstant(ctx, t, engRaw, raw, tt.query, evalT)

			sortVector(got)
			sortVector(want)
			a.Len(got, len(want), "pushdown and raw must return the same series count")
			for i := range got {
				assert.Equal(t, want[i].Metric, got[i].Metric, "series labels mismatch")
				assert.InDelta(t, want[i].F, got[i].F, 1e-9, "value mismatch for %s", got[i].Metric)
			}
		})
	}
}

// TestOverTimePushdownExactValues checks the pushdown computes the documented aggregates for known
// samples, including the exclusive lower-bound window edge (the -30s sample is outside a [30s]
// window) and the name-drop a range-vector function applies.
func TestOverTimePushdownExactValues(t *testing.T) {
	ctx := context.Background()
	evalT := time.Unix(2_000_000, 0).UTC()

	store := ingestGauge(ctx, t, evalT, []timeSample{
		{-30 * time.Second, 10},
		{-20 * time.Second, 20},
		{-10 * time.Second, 30},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store) // pushdown on
	eng, err := otelpromql.New(b, otelpromql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name  string
		query string
		want  float64
	}{
		// 30s window excludes the -30s (value 10) sample: in-window values are {20, 30}.
		{"Count", "count_over_time(ot_gauge[30s])", 2},
		{"Sum", "sum_over_time(ot_gauge[30s])", 50},
		{"Min", "min_over_time(ot_gauge[30s])", 20},
		{"Max", "max_over_time(ot_gauge[30s])", 30},
		{"Avg", "avg_over_time(ot_gauge[30s])", 25},
		{"Present", "present_over_time(ot_gauge[30s])", 1},
		// 40s window includes all three: {10, 20, 30}.
		{"CountFull", "count_over_time(ot_gauge[40s])", 3},
		{"SumFull", "sum_over_time(ot_gauge[40s])", 60},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)

			vec := execInstant(ctx, t, eng, b, tt.query, evalT)
			a.Len(vec, 1, "one series")
			a.Equal(tt.want, vec[0].F)
			a.Empty(vec[0].Metric.Get("__name__"), "range-vector functions drop the metric name")
		})
	}
}

func execInstant(
	ctx context.Context, t *testing.T, eng otelpromql.Engine,
	b *storagebackend.Backend, query string, ts time.Time,
) promql.Vector {
	t.Helper()

	q, err := eng.NewInstantQuery(ctx, b, nil, query, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)

	return vec
}

// gaugeSeries is one labeled series of the multi-series gauge fixture: a "foo" label value and the
// (offset-from-evalT, value) samples that series carries.
type gaugeSeries struct {
	foo     string
	samples []timeSample
}

// ingestGaugeSeries writes the "ot_gauge" gauge with one series per entry (distinguished by the
// label foo=<entry.foo>), each carrying its own samples, and returns the populated store. It is the
// multi-series analog of [ingestGauge], so a test can exercise per-series folding, label
// rendering, matcher subsetting, and result ordering — not just a single series.
func ingestGaugeSeries(ctx context.Context, t *testing.T, evalT time.Time, series []gaugeSeries) *storage.Storage {
	t.Helper()

	store, err := storage.InMemory()
	require.NoError(t, err)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("ot_gauge")
	g := m.SetEmptyGauge()
	for _, s := range series {
		for _, smp := range s.samples {
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.Timestamp(evalT.Add(smp.offset).UnixNano()))
			dp.SetDoubleValue(smp.value)
			dp.Attributes().PutStr("foo", s.foo)
		}
	}

	b := storagebackend.New(store)
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	return store
}

// rampSamples builds samples at every step in [start, end] (offsets from evalT, typically negative),
// with values base, base+1, … so each series carries a distinct, monotonic curve.
func rampSamples(start, end, step time.Duration, base float64) []timeSample {
	var out []timeSample
	v := base
	for off := start; off <= end; off += step {
		out = append(out, timeSample{offset: off, value: v})
		v++
	}
	return out
}

func newOverTimeEngine(t *testing.T, b *storagebackend.Backend) otelpromql.Engine {
	t.Helper()
	eng, err := otelpromql.New(b, otelpromql.EngineOpts{
		MaxSamples: 1_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)
	return eng
}

// diffBackends pairs a pushdown-on and a pushdown-off backend over the same store, each with its own
// engine, so a query can be evaluated both ways and the results compared. The pushdown-off path is
// the standard Prometheus matrix-selector fold — the oracle the sidecar pushdown must match.
type diffBackends struct {
	ctx    context.Context
	pB, rB *storagebackend.Backend
	pE, rE otelpromql.Engine
}

func newDiffBackends(ctx context.Context, t *testing.T, store *storage.Storage) diffBackends {
	t.Helper()
	pB := storagebackend.New(store)                                             // pushdown on (default)
	rB := storagebackend.New(store, storagebackend.WithOverTimePushdown(false)) // raw fold (oracle)
	return diffBackends{ctx: ctx, pB: pB, rB: rB, pE: newOverTimeEngine(t, pB), rE: newOverTimeEngine(t, rB)}
}

func (d diffBackends) assertInstant(t *testing.T, query string, ts time.Time) {
	t.Helper()
	got := execInstant(d.ctx, t, d.pE, d.pB, query, ts)
	want := execInstant(d.ctx, t, d.rE, d.rB, query, ts)
	assertVectorEqual(t, query, got, want)
}

func (d diffBackends) assertRange(t *testing.T, query string, start, end time.Time, step time.Duration) {
	t.Helper()
	got := execRange(d.ctx, t, d.pE, d.pB, query, start, end, step)
	want := execRange(d.ctx, t, d.rE, d.rB, query, start, end, step)
	assertMatrixEqual(t, query, got, want)
}

func execRange(
	ctx context.Context, t *testing.T, eng otelpromql.Engine,
	b *storagebackend.Backend, query string, start, end time.Time, step time.Duration,
) promql.Matrix {
	t.Helper()

	q, err := eng.NewRangeQuery(ctx, b, nil, query, start, end, step)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	m, err := res.Matrix()
	require.NoError(t, err)

	return m
}

func assertVectorEqual(t *testing.T, query string, got, want promql.Vector) {
	t.Helper()
	sortVector(got)
	sortVector(want)
	require.Lenf(t, got, len(want), "%s: series count", query)
	for i := range got {
		assert.Equalf(t, want[i].Metric, got[i].Metric, "%s: series labels", query)
		assertFloatEqual(t, query, got[i].Metric, want[i].F, got[i].F)
	}
}

func assertMatrixEqual(t *testing.T, query string, got, want promql.Matrix) {
	t.Helper()
	sort.Sort(got)
	sort.Sort(want)
	require.Lenf(t, got, len(want), "%s: series count", query)
	for i := range got {
		assert.Equalf(t, want[i].Metric, got[i].Metric, "%s: series labels", query)
		require.Lenf(t, got[i].Floats, len(want[i].Floats), "%s: point count for %s", query, got[i].Metric)
		for j := range got[i].Floats {
			assert.Equalf(t, want[i].Floats[j].T, got[i].Floats[j].T, "%s: step timestamp for %s", query, got[i].Metric)
			assertFloatEqual(t, query, got[i].Metric, want[i].Floats[j].F, got[i].Floats[j].F)
		}
	}
}

func assertFloatEqual(t *testing.T, query string, lset labels.Labels, want, got float64) {
	t.Helper()
	if math.IsNaN(want) {
		assert.Truef(t, math.IsNaN(got), "%s: want NaN, got %v for %s", query, got, lset)
		return
	}
	assert.InDeltaf(t, want, got, 1e-9, "%s: value for %s", query, lset)
}

// instantPlan returns the execution plan of an instant query, for asserting whether the aggregate
// pushdown operator was selected.
func instantPlan(
	ctx context.Context, t *testing.T, eng otelpromql.Engine,
	b *storagebackend.Backend, query string, ts time.Time,
) *promqlengine.ExplainOutputNode {
	t.Helper()
	q, err := eng.NewInstantQuery(ctx, b, nil, query, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)
	exp, ok := q.(promqlengine.ExplainableQuery)
	require.True(t, ok, "query must implement ExplainableQuery to inspect its plan")
	return exp.Explain()
}

// planOpName is the operator-name fragment the aggregate-pushdown vector operator reports in the
// query plan (see aggregateOverTimeOp.String).
const planOpName = "aggregateOverTimeOp"

func explainContains(node *promqlengine.ExplainOutputNode, substr string) bool {
	if node == nil {
		return false
	}
	if strings.Contains(node.OperatorName, substr) {
		return true
	}
	for i := range node.Children {
		if explainContains(&node.Children[i], substr) {
			return true
		}
	}
	return false
}

// TestOverTimePushdownPlanRouting proves the pushdown is actually exercised where it should be and
// — just as important for validity — that it cleanly steps aside everywhere it must. Without this,
// the differential tests could pass trivially by never pushing down at all.
func TestOverTimePushdownPlanRouting(t *testing.T) {
	ctx := context.Background()
	evalT := time.Unix(2_000_000, 0).UTC()

	store := ingestGaugeSeries(ctx, t, evalT, []gaugeSeries{
		{foo: "a", samples: []timeSample{{-10 * time.Second, 1}, {-5 * time.Second, 2}}},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	on := storagebackend.New(store)
	off := storagebackend.New(store, storagebackend.WithOverTimePushdown(false))
	engOn := newOverTimeEngine(t, on)
	engOff := newOverTimeEngine(t, off)

	t.Run("SupportedInstantUsesSidecar", func(t *testing.T) {
		for _, q := range []string{
			"count_over_time(ot_gauge[30s])",
			"sum_over_time(ot_gauge[30s])",
			"min_over_time(ot_gauge[30s])",
			"max_over_time(ot_gauge[30s])",
			"avg_over_time(ot_gauge[30s])",
			"present_over_time(ot_gauge[30s])",
			`max_over_time(ot_gauge{foo="a"}[30s])`,
			"count_over_time(ot_gauge[30s] offset 10s)",
		} {
			plan := instantPlan(ctx, t, engOn, on, q, evalT)
			assert.Truef(t, explainContains(plan, planOpName), "%s should push down to the sidecar", q)
		}
	})

	t.Run("DisabledFallsBack", func(t *testing.T) {
		plan := instantPlan(ctx, t, engOff, off, "count_over_time(ot_gauge[30s])", evalT)
		assert.False(t, explainContains(plan, planOpName), "WithOverTimePushdown(false) must not use the sidecar")
	})

	t.Run("UnsupportedFunctionFallsBack", func(t *testing.T) {
		// Functions the sidecar (count/sum/min/max) cannot answer must keep the raw matrix selector.
		for _, q := range []string{
			"last_over_time(ot_gauge[30s])",
			"stddev_over_time(ot_gauge[30s])",
			"quantile_over_time(0.5, ot_gauge[30s])",
			"changes(ot_gauge[30s])",
			"rate(ot_gauge[30s])",
		} {
			plan := instantPlan(ctx, t, engOn, on, q, evalT)
			assert.Falsef(t, explainContains(plan, planOpName), "%s must fall back to the matrix selector", q)
		}
	})

	t.Run("RangeQueryFallsBack", func(t *testing.T) {
		// Range queries re-evaluate over a sliding window the sidecar's step buckets don't model.
		q, err := engOn.NewRangeQuery(ctx, on, nil, "count_over_time(ot_gauge[30s])", evalT, evalT.Add(time.Minute), 15*time.Second)
		require.NoError(t, err)
		t.Cleanup(q.Close)
		exp, ok := q.(promqlengine.ExplainableQuery)
		require.True(t, ok)
		assert.False(t, explainContains(exp.Explain(), planOpName), "range query must use the matrix selector")
	})
}

// TestOverTimePushdownMultiSeriesMatchesRaw is the differential oracle over a multi-series fixture:
// for a broad query matrix (every supported fold, matcher subsets, offsets, downstream aggregation,
// arithmetic, and an empty result) the pushdown must produce exactly what the raw matrix-selector
// fold produces — same series, same labels, same values. This exercises per-series folding, label
// rendering, post-fetch matcher re-checks, window-edge inclusion, and result ordering at once.
func TestOverTimePushdownMultiSeriesMatchesRaw(t *testing.T) {
	ctx := context.Background()
	evalT := time.Unix(2_000_000, 0).UTC()

	store := ingestGaugeSeries(ctx, t, evalT, []gaugeSeries{
		// a: -30s sample is excluded by a 30s window (exclusive lower bound), included by 60s.
		{foo: "a", samples: []timeSample{{-30 * time.Second, 10}, {-20 * time.Second, 20}, {-10 * time.Second, 30}}},
		{foo: "b", samples: []timeSample{{-25 * time.Second, 5}, {-5 * time.Second, 7}}},
		// c: only sample sits outside a 30s window, so c is present only for wider windows.
		{foo: "c", samples: []timeSample{{-45 * time.Second, 100}}},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	d := newDiffBackends(ctx, t, store)

	for _, q := range []string{
		// Per-series folds, narrow window (c absent, a drops its -30s sample).
		"count_over_time(ot_gauge[30s])",
		"sum_over_time(ot_gauge[30s])",
		"min_over_time(ot_gauge[30s])",
		"max_over_time(ot_gauge[30s])",
		"avg_over_time(ot_gauge[30s])",
		"present_over_time(ot_gauge[30s])",
		// Wider window pulls c in and a's boundary sample.
		"count_over_time(ot_gauge[60s])",
		"sum_over_time(ot_gauge[60s])",
		"avg_over_time(ot_gauge[60s])",
		// Matcher subsets: equality, negation, regex, negated regex — index-safe and re-checked.
		`count_over_time(ot_gauge{foo="a"}[30s])`,
		`count_over_time(ot_gauge{foo!="a"}[30s])`,
		`sum_over_time(ot_gauge{foo=~"a|b"}[30s])`,
		`sum_over_time(ot_gauge{foo!~"a"}[60s])`,
		// Offset shifts the evaluated window back in time.
		"count_over_time(ot_gauge[30s] offset 10s)",
		"sum_over_time(ot_gauge[30s] offset 20s)",
		// Downstream aggregation composes with the pushdown operator.
		"sum(sum_over_time(ot_gauge[30s]))",
		"sum by (foo) (count_over_time(ot_gauge[60s]))",
		"count(present_over_time(ot_gauge[60s]))",
		"max(max_over_time(ot_gauge[60s]))",
		"avg(avg_over_time(ot_gauge[60s]))",
		// Arithmetic on top of the range-vector function.
		"avg_over_time(ot_gauge[60s]) * 2",
		// Empty result (no series matches).
		`count_over_time(ot_gauge{foo="zzz"}[30s])`,
	} {
		t.Run(q, func(t *testing.T) { d.assertInstant(t, q, evalT) })
	}
}

// TestOverTimePushdownUnsupportedMatchesRaw checks that functions the pushdown does not cover still
// produce the correct result through the fallback path on a pushdown-enabled backend — i.e. routing
// them away from the sidecar does not perturb their values.
func TestOverTimePushdownUnsupportedMatchesRaw(t *testing.T) {
	ctx := context.Background()
	evalT := time.Unix(2_000_000, 0).UTC()

	store := ingestGaugeSeries(ctx, t, evalT, []gaugeSeries{
		{foo: "a", samples: []timeSample{{-50 * time.Second, 4}, {-30 * time.Second, 9}, {-10 * time.Second, 2}}},
		{foo: "b", samples: []timeSample{{-40 * time.Second, 1}, {-20 * time.Second, 8}}},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	d := newDiffBackends(ctx, t, store)

	for _, q := range []string{
		"last_over_time(ot_gauge[60s])",
		"stddev_over_time(ot_gauge[60s])",
		"stdvar_over_time(ot_gauge[60s])",
		"quantile_over_time(0.9, ot_gauge[60s])",
		"changes(ot_gauge[60s])",
		"delta(ot_gauge[60s])",
	} {
		t.Run(q, func(t *testing.T) { d.assertInstant(t, q, evalT) })
	}
}

// TestOverTimePushdownRangeMatchesRaw confirms that range queries — which always take the raw path,
// since the sidecar models instant evaluation only — are unaffected by enabling the pushdown: a
// pushdown-on backend returns the same matrix as a vanilla one across every step.
func TestOverTimePushdownRangeMatchesRaw(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(2_000_000, 0).UTC()

	store := ingestGaugeSeries(ctx, t, base, []gaugeSeries{
		{foo: "a", samples: rampSamples(-120*time.Second, 0, 10*time.Second, 1)},
		{foo: "b", samples: rampSamples(-120*time.Second, 0, 20*time.Second, 50)},
	})
	t.Cleanup(func() { _ = store.Close(ctx) })

	d := newDiffBackends(ctx, t, store)

	start, end, step := base.Add(-90*time.Second), base, 15*time.Second
	for _, q := range []string{
		"count_over_time(ot_gauge[30s])",
		"sum_over_time(ot_gauge[40s])",
		"avg_over_time(ot_gauge[30s])",
		"sum by (foo) (max_over_time(ot_gauge[40s]))",
	} {
		t.Run(q, func(t *testing.T) { d.assertRange(t, q, start, end, step) })
	}
}
