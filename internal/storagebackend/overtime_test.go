package storagebackend_test

import (
	"context"
	"slices"
	"testing"
	"time"

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
