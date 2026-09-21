package otlpdirect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

func marshalMetrics(tb testing.TB, md pmetric.Metrics) []byte {
	tb.Helper()

	raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	require.NoError(tb, err)

	return raw
}

// convertBothMetrics decodes md directly and via the pdata path, canonicalized for comparison.
func convertBothMetrics(
	tb testing.TB, md pmetric.Metrics,
) (direct, viaPdata *metric.Metrics, dropped otlpdirect.Dropped, pdataDropped pdataconv.Dropped) {
	tb.Helper()

	var c otlpdirect.MetricsConverter

	direct, dropped, err := c.Convert(marshalMetrics(tb, md))
	require.NoError(tb, err)

	viaPdata = &metric.Metrics{}
	pdataDropped = pdataconv.AppendMetrics(viaPdata, md)

	return canonicalMetrics(direct), canonicalMetrics(viaPdata), dropped, pdataDropped
}

// requireSameMetrics asserts both paths agree on the batch and on what they dropped.
func requireSameMetrics(tb testing.TB, md pmetric.Metrics) *metric.Metrics {
	tb.Helper()

	got, _ := requireSameMetricsDropped(tb, md)

	return got
}

// requireSameMetricsDropped is [requireSameMetrics] also handing back what was dropped.
func requireSameMetricsDropped(tb testing.TB, md pmetric.Metrics) (*metric.Metrics, otlpdirect.Dropped) {
	tb.Helper()

	direct, viaPdata, dropped, pdataDropped := convertBothMetrics(tb, md)
	require.Equal(tb, viaPdata, direct)
	require.Equal(tb, pdataDropped.Points, dropped.Points, "dropped point counts must agree")
	require.Equal(tb, pdataDropped.Exemplars, dropped.Exemplars, "dropped exemplar counts must agree")

	return direct, dropped
}

func newGauge(md pmetric.Metrics, name string) pmetric.Metric {
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)

	return m
}

func TestConvertMetricsGaugeAndSum(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://schema.example/resource")
	rm.Resource().Attributes().PutStr("service.name", "api")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://schema.example/scope")
	sm.Scope().SetName("go.opentelemetry.io/example")
	sm.Scope().SetVersion("1.2.3")

	g := sm.Metrics().AppendEmpty()
	g.SetName("process.cpu.utilization")
	g.SetUnit("1")

	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetStartTimestamp(1_700_000_000_000_000_000)
	gp.SetTimestamp(1_700_000_000_000_000_001)
	gp.SetDoubleValue(0.42)
	gp.Attributes().PutStr("cpu", "0")

	s := sm.Metrics().AppendEmpty()
	s.SetName("http.server.request.count")
	s.SetUnit("{request}")

	sum := s.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	sp := sum.DataPoints().AppendEmpty()
	sp.SetStartTimestamp(1_700_000_000_000_000_000)
	sp.SetTimestamp(1_700_000_000_000_000_002)
	sp.SetIntValue(1234)
	sp.Attributes().PutStr("http.route", "/things")

	requireSameMetrics(t, md)
}

// TestConvertMetricsTemporalityAndMonotonicity walks the qualifiers that are part of a sum's
// identity, so a mix-up changes which series a point lands in.
func TestConvertMetricsTemporalityAndMonotonicity(t *testing.T) {
	t.Parallel()

	for _, temp := range []pmetric.AggregationTemporality{
		pmetric.AggregationTemporalityUnspecified,
		pmetric.AggregationTemporalityDelta,
		pmetric.AggregationTemporalityCumulative,
	} {
		for _, monotonic := range []bool{false, true} {
			md := pmetric.NewMetrics()

			m := newGauge(md, "counter")
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(temp)
			sum.SetIsMonotonic(monotonic)

			p := sum.DataPoints().AppendEmpty()
			p.SetTimestamp(1)
			p.SetIntValue(7)

			requireSameMetrics(t, md)
		}
	}
}

// TestConvertMetricsDropsValuelessPoint pins the one unrepresentable case: a number point with
// neither as_double nor as_int is counted, not stored as zero.
func TestConvertMetricsDropsValuelessPoint(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	m := newGauge(md, "empty")
	dps := m.SetEmptyGauge().DataPoints()

	none := dps.AppendEmpty()
	none.SetTimestamp(1)

	valued := dps.AppendEmpty()
	valued.SetTimestamp(2)
	valued.SetDoubleValue(1)

	direct, viaPdata, dropped, pdataDropped := convertBothMetrics(t, md)
	require.Equal(t, viaPdata, direct)
	assert.Equal(t, 1, dropped.Points)
	assert.Equal(t, pdataDropped.Points, dropped.Points)

	assert.Len(t, direct.Resources[0].Scopes[0].Metrics[0].Points, 1)
}

// exemplarFixture builds one exemplar on a number data point.
type exemplarFixture struct {
	ts       pcommon.Timestamp
	value    func(pmetric.Exemplar)
	traceID  pcommon.TraceID
	spanID   pcommon.SpanID
	filtered map[string]string
}

func (f exemplarFixture) appendTo(exs pmetric.ExemplarSlice) {
	e := exs.AppendEmpty()
	e.SetTimestamp(f.ts)

	if f.value != nil {
		f.value(e)
	}

	e.SetTraceID(f.traceID)
	e.SetSpanID(f.spanID)

	for k, v := range f.filtered {
		e.FilteredAttributes().PutStr(k, v)
	}
}

func doubleExemplar(v float64) func(pmetric.Exemplar) {
	return func(e pmetric.Exemplar) { e.SetDoubleValue(v) }
}

func intExemplar(v int64) func(pmetric.Exemplar) {
	return func(e pmetric.Exemplar) { e.SetIntValue(v) }
}

var (
	fixtureTraceID = pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	fixtureSpanID  = pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
)

// TestConvertMetricsExemplars pins the parity of the exemplars hanging off gauge and sum points,
// which the direct decoder must reproduce byte for byte — including which ones pdataconv refuses.
func TestConvertMetricsExemplars(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		exemplars []exemplarFixture
		// wantExemplars is how many survive on the point, and wantDropped how many do not.
		wantExemplars int
		wantDropped   int
	}{
		{
			name: "trace context",
			exemplars: []exemplarFixture{
				{ts: 10, value: doubleExemplar(1.5), traceID: fixtureTraceID, spanID: fixtureSpanID},
			},
			wantExemplars: 1,
		},
		{
			name:          "no trace context",
			exemplars:     []exemplarFixture{{ts: 11, value: doubleExemplar(2.5)}},
			wantExemplars: 1,
		},
		{
			name: "span id only",
			exemplars: []exemplarFixture{
				{ts: 12, value: intExemplar(7), spanID: fixtureSpanID},
			},
			wantExemplars: 1,
		},
		{
			name: "filtered attributes",
			exemplars: []exemplarFixture{
				{
					ts:       13,
					value:    doubleExemplar(3),
					traceID:  fixtureTraceID,
					spanID:   fixtureSpanID,
					filtered: map[string]string{"http.route": "/things", "cpu": "0"},
				},
			},
			wantExemplars: 1,
		},
		{
			name:        "value-less",
			exemplars:   []exemplarFixture{{ts: 14, traceID: fixtureTraceID}},
			wantDropped: 1,
		},
		{
			name: "mixed",
			exemplars: []exemplarFixture{
				{ts: 15, value: intExemplar(1), traceID: fixtureTraceID, spanID: fixtureSpanID},
				{ts: 16},
				{ts: 17, value: doubleExemplar(2), filtered: map[string]string{"k": "v"}},
			},
			wantExemplars: 2,
			wantDropped:   1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, kind := range []string{"gauge", "sum"} {
				t.Run(kind, func(t *testing.T) {
					t.Parallel()

					md := pmetric.NewMetrics()

					m := newGauge(md, "requests")

					var dp pmetric.NumberDataPoint
					if kind == "gauge" {
						dp = m.SetEmptyGauge().DataPoints().AppendEmpty()
					} else {
						sum := m.SetEmptySum()
						sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
						sum.SetIsMonotonic(true)
						dp = sum.DataPoints().AppendEmpty()
					}

					dp.SetTimestamp(100)
					dp.SetDoubleValue(9)
					dp.Attributes().PutStr("http.route", "/things")

					for _, f := range tt.exemplars {
						f.appendTo(dp.Exemplars())
					}

					got, dropped := requireSameMetricsDropped(t, md)
					assert.Equal(t, tt.wantDropped, dropped.Exemplars)
					assert.Zero(t, dropped.Points)

					point := got.Resources[0].Scopes[0].Metrics[0].Points[0]
					assert.Len(t, point.Exemplars, tt.wantExemplars)
				})
			}
		})
	}
}

// TestConvertMetricsExemplarsOnDroppedPoint pins that a value-less point takes its exemplars with
// it: they are counted, never reattached to another point.
func TestConvertMetricsExemplarsOnDroppedPoint(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	dps := newGauge(md, "g").SetEmptyGauge().DataPoints()

	none := dps.AppendEmpty()
	none.SetTimestamp(1)
	exemplarFixture{ts: 1, value: doubleExemplar(1), traceID: fixtureTraceID}.appendTo(none.Exemplars())
	exemplarFixture{ts: 2}.appendTo(none.Exemplars())

	valued := dps.AppendEmpty()
	valued.SetTimestamp(2)
	valued.SetDoubleValue(1)
	exemplarFixture{ts: 3, value: doubleExemplar(4), spanID: fixtureSpanID}.appendTo(valued.Exemplars())

	got, dropped := requireSameMetricsDropped(t, md)
	assert.Equal(t, 1, dropped.Points)
	assert.Equal(t, 2, dropped.Exemplars)

	points := got.Resources[0].Scopes[0].Metrics[0].Points
	require.Len(t, points, 1)
	assert.Len(t, points[0].Exemplars, 1)
}

// TestConvertMetricsDecomposedExemplarsDropped pins the accounting for the shapes stored by classic
// decomposition: one point becomes several series, so its exemplars have no home.
func TestConvertMetricsDecomposedExemplarsDropped(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		build       func(pmetric.Metric) pmetric.ExemplarSlice
		wantDropped int
	}{
		{
			name: "histogram",
			build: func(m pmetric.Metric) pmetric.ExemplarSlice {
				h := m.SetEmptyHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(1)
				dp.SetCount(3)
				dp.ExplicitBounds().FromRaw([]float64{1, 2})
				dp.BucketCounts().FromRaw([]uint64{1, 1, 1})

				return dp.Exemplars()
			},
			wantDropped: 3,
		},
		{
			name: "exponential histogram",
			build: func(m pmetric.Metric) pmetric.ExemplarSlice {
				eh := m.SetEmptyExponentialHistogram()
				eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

				dp := eh.DataPoints().AppendEmpty()
				dp.SetTimestamp(1)
				dp.SetCount(2)
				dp.SetScale(1)
				dp.Positive().BucketCounts().FromRaw([]uint64{1, 1})

				return dp.Exemplars()
			},
			wantDropped: 3,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			md := pmetric.NewMetrics()

			exs := tt.build(newGauge(md, "h"))

			exemplarFixture{ts: 1, value: doubleExemplar(1), traceID: fixtureTraceID}.appendTo(exs)
			exemplarFixture{ts: 2, value: intExemplar(2), spanID: fixtureSpanID}.appendTo(exs)
			// A value-less one on a decomposed point is dropped for both reasons, counted once.
			exemplarFixture{ts: 3}.appendTo(exs)

			_, dropped := requireSameMetricsDropped(t, md)
			assert.Equal(t, tt.wantDropped, dropped.Exemplars)
			assert.Zero(t, dropped.Points)
		})
	}
}

// TestConvertMetricsHistogram covers classic decomposition into _count/_sum/_bucket{le}.
func TestConvertMetricsHistogram(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		hasSum bool
		temp   pmetric.AggregationTemporality
	}{
		{"cumulative with sum", true, pmetric.AggregationTemporalityCumulative},
		{"delta with sum", true, pmetric.AggregationTemporalityDelta},
		{"cumulative without sum", false, pmetric.AggregationTemporalityCumulative},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			md := pmetric.NewMetrics()

			m := newGauge(md, "http.server.duration")
			m.SetUnit("ms")

			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(tt.temp)

			dp := h.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(1_700_000_000_000_000_000)
			dp.SetTimestamp(1_700_000_000_000_000_001)
			dp.SetCount(10)
			dp.Attributes().PutStr("http.route", "/things")
			dp.ExplicitBounds().FromRaw([]float64{0.005, 0.01, 0.025, 2.5})
			dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 3, 1})

			if tt.hasSum {
				dp.SetSum(1.5)
			}

			got := requireSameMetrics(t, md)

			// The decomposition must produce _count, optionally _sum, and one _bucket per bucket
			// count — the last of which is the +Inf overflow.
			want := 1 + len(dp.BucketCounts().AsRaw())
			if tt.hasSum {
				want++
			}

			assert.Len(t, got.Resources[0].Scopes[0].Metrics, want)
		})
	}
}

// TestConvertMetricsExpHistogram covers the exponential form, whose buckets are derived from the
// scale rather than carried explicitly.
func TestConvertMetricsExpHistogram(t *testing.T) {
	t.Parallel()

	for _, scale := range []int32{-2, 0, 1, 3} {
		md := pmetric.NewMetrics()

		m := newGauge(md, "rpc.duration")
		m.SetUnit("s")

		eh := m.SetEmptyExponentialHistogram()
		eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dp := eh.DataPoints().AppendEmpty()
		dp.SetTimestamp(1)
		dp.SetCount(12)
		dp.SetSum(3.5)
		dp.SetScale(scale)
		dp.SetZeroCount(2)
		dp.Positive().SetOffset(1)
		dp.Positive().BucketCounts().FromRaw([]uint64{3, 0, 4})
		dp.Negative().SetOffset(-2)
		dp.Negative().BucketCounts().FromRaw([]uint64{2, 1})
		dp.Attributes().PutStr("rpc.method", "Get")

		requireSameMetrics(t, md)
	}
}

// TestConvertMetricsExpHistogramEmptyBuckets covers the degenerate point a real SDK emits before
// it has observed anything.
func TestConvertMetricsExpHistogramEmptyBuckets(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	m := newGauge(md, "rpc.duration")

	eh := m.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := eh.DataPoints().AppendEmpty()
	dp.SetTimestamp(1)

	requireSameMetrics(t, md)
}

// TestConvertMetricsSummary covers _count/_sum plus the per-quantile gauges.
func TestConvertMetricsSummary(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	m := newGauge(md, "rpc.duration")
	m.SetUnit("ms")

	dp := m.SetEmptySummary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(1)
	dp.SetTimestamp(2)
	dp.SetCount(9)
	dp.SetSum(2.25)
	dp.Attributes().PutStr("rpc.service", "Things")

	for _, q := range []struct{ quantile, value float64 }{
		{0, 0.1}, {0.5, 1.0}, {0.99, 9.5}, {1, 12},
	} {
		qv := dp.QuantileValues().AppendEmpty()
		qv.SetQuantile(q.quantile)
		qv.SetValue(q.value)
	}

	got := requireSameMetrics(t, md)
	assert.Len(t, got.Resources[0].Scopes[0].Metrics, 2+4)
}

// TestConvertMetricsHistogramBoundFormatting pins the `le` label text, which is what a query
// matches on — a different float rendering silently makes a different series.
func TestConvertMetricsHistogramBoundFormatting(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()

	m := newGauge(md, "h")

	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := h.DataPoints().AppendEmpty()
	dp.SetTimestamp(1)
	dp.SetCount(4)
	dp.ExplicitBounds().FromRaw([]float64{0.0001, 1, 1e21, 0.30000000000000004})
	dp.BucketCounts().FromRaw([]uint64{1, 1, 1, 1, 0})

	got := requireSameMetrics(t, md)

	var les []string

	for _, mt := range got.Resources[0].Scopes[0].Metrics {
		if string(mt.Name) != "h_bucket" {
			continue
		}

		v, ok := mt.Points[0].Attributes.Get([]byte("le"))
		require.True(t, ok)
		les = append(les, string(v.Str()))
	}

	assert.Equal(t, []string{"0.0001", "1", "1e+21", "0.30000000000000004", "+Inf"}, les)
}

// TestConvertMetricsMixedAndReuse pins that the scratch reused across points does not bleed one
// point's attributes, bounds or bucket counts into the next.
func TestConvertMetricsMixedAndReuse(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	for i := range 5 {
		g := sm.Metrics().AppendEmpty()
		g.SetName("g")

		gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
		gp.SetTimestamp(pcommon.Timestamp(i + 1))
		gp.SetDoubleValue(float64(i))
		gp.Attributes().PutInt("i", int64(i))

		h := sm.Metrics().AppendEmpty()
		h.SetName("h")

		hist := h.SetEmptyHistogram()
		hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		hp := hist.DataPoints().AppendEmpty()
		hp.SetTimestamp(pcommon.Timestamp(i + 1))
		hp.SetCount(uint64(i))

		// Bucket counts of differing length, so a leaked scratch shows up as a bucket the point
		// never had.
		bounds := make([]float64, i)
		counts := make([]uint64, i+1)

		for b := range i {
			bounds[b] = float64(b + 1)
			counts[b] = uint64(b)
		}

		hp.ExplicitBounds().FromRaw(bounds)
		hp.BucketCounts().FromRaw(counts)
	}

	requireSameMetrics(t, md)
}

func TestConvertMetricsEmpty(t *testing.T) {
	t.Parallel()

	var c otlpdirect.MetricsConverter

	got, dropped, err := c.Convert(nil)
	require.NoError(t, err)
	assert.Zero(t, dropped)
	assert.Empty(t, got.Resources)

	requireSameMetrics(t, pmetric.NewMetrics())
}

// TestConvertMetricsNoDataArm covers a Metric whose oneof is unset, which carries no points.
func TestConvertMetricsNoDataArm(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	newGauge(md, "nothing")

	requireSameMetrics(t, md)
}

func TestConvertMetricsReuseIsIsolated(t *testing.T) {
	t.Parallel()

	var c otlpdirect.MetricsConverter

	first := pmetric.NewMetrics()

	fh := newGauge(first, "h")
	fhist := fh.SetEmptyHistogram()
	fhist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	fp := fhist.DataPoints().AppendEmpty()
	fp.SetTimestamp(1)
	fp.SetCount(3)
	fp.SetSum(1)
	fp.ExplicitBounds().FromRaw([]float64{1, 2, 3})
	fp.BucketCounts().FromRaw([]uint64{1, 1, 1, 0})
	fp.Attributes().PutStr("a", "1")

	second := pmetric.NewMetrics()

	sg := newGauge(second, "g")

	sp := sg.SetEmptyGauge().DataPoints().AppendEmpty()
	sp.SetTimestamp(2)
	sp.SetDoubleValue(5)

	_, _, err := c.Convert(marshalMetrics(t, first))
	require.NoError(t, err)

	got, _, err := c.Convert(marshalMetrics(t, second))
	require.NoError(t, err)

	want := &metric.Metrics{}
	pdataconv.AppendMetrics(want, second)
	assert.Equal(t, canonicalMetrics(want), canonicalMetrics(got))
}

func BenchmarkConvertMetrics(b *testing.B) {
	md := pmetric.NewMetrics()

	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("bench")

	for i := range 500 {
		g := sm.Metrics().AppendEmpty()
		g.SetName("process.cpu.utilization")
		g.SetUnit("1")

		gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
		gp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + i))
		gp.SetDoubleValue(float64(i))
		gp.Attributes().PutStr("cpu", "0")

		h := sm.Metrics().AppendEmpty()
		h.SetName("http.server.duration")
		h.SetUnit("ms")

		hist := h.SetEmptyHistogram()
		hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		hp := hist.DataPoints().AppendEmpty()
		hp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + i))
		hp.SetCount(uint64(i))
		hp.SetSum(float64(i))
		hp.ExplicitBounds().FromRaw([]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
		hp.BucketCounts().FromRaw([]uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
		hp.Attributes().PutStr("http.route", "/api/v1/things")
	}

	raw := marshalMetrics(b, md)

	b.Run("Direct", func(b *testing.B) {
		var c otlpdirect.MetricsConverter

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			if _, _, err := c.Convert(raw); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Pdata", func(b *testing.B) {
		var u pmetric.ProtoUnmarshaler

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			decoded, err := u.UnmarshalMetrics(raw)
			if err != nil {
				b.Fatal(err)
			}

			dst := &metric.Metrics{}
			pdataconv.AppendMetrics(dst, decoded)
		}
	})
}
