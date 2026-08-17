package otlpdirect_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// FuzzConvertMetrics drives the metric decoder with arbitrary bytes. Metrics is the only signal
// that computes rather than copies: bucket bounds are derived from a scale, counts are cumulated,
// and names are synthesized — so a malformed point can reach arithmetic the other decoders never
// run, including a scale that makes math.Pow overflow.
func FuzzConvertMetrics(f *testing.F) {
	seed := func(build func(pmetric.Metrics)) {
		md := pmetric.NewMetrics()
		build(md)

		raw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
		if err != nil {
			f.Fatal(err)
		}

		f.Add(raw)
	}

	seed(func(pmetric.Metrics) {})

	seed(func(md pmetric.Metrics) {
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("g")

		p := m.SetEmptyGauge().DataPoints().AppendEmpty()
		p.SetTimestamp(1)
		p.SetDoubleValue(1.5)
	})

	seed(func(md pmetric.Metrics) {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("res")
		rm.Resource().Attributes().PutStr("service.name", "api")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.SetSchemaUrl("scope")
		sm.Scope().SetName("n")

		s := sm.Metrics().AppendEmpty()
		s.SetName("c")
		s.SetUnit("1")

		sum := s.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)

		sp := sum.DataPoints().AppendEmpty()
		sp.SetStartTimestamp(1)
		sp.SetTimestamp(2)
		sp.SetIntValue(3)
		sp.Attributes().PutStr("k", "v")
	})

	seed(func(md pmetric.Metrics) {
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("h")
		m.SetUnit("ms")

		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(1)
		dp.SetCount(6)
		dp.SetSum(1.5)
		dp.ExplicitBounds().FromRaw([]float64{1, 2, 3})
		dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 0})
		dp.Attributes().PutStr("k", "v")
	})

	seed(func(md pmetric.Metrics) {
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("e")

		eh := m.SetEmptyExponentialHistogram()
		eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dp := eh.DataPoints().AppendEmpty()
		dp.SetTimestamp(1)
		dp.SetCount(9)
		dp.SetSum(2)
		dp.SetScale(3)
		dp.SetZeroCount(1)
		dp.Positive().SetOffset(2)
		dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
		dp.Negative().SetOffset(-3)
		dp.Negative().BucketCounts().FromRaw([]uint64{2})
	})

	seed(func(md pmetric.Metrics) {
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("s")

		dp := m.SetEmptySummary().DataPoints().AppendEmpty()
		dp.SetTimestamp(1)
		dp.SetCount(4)
		dp.SetSum(2)

		for _, q := range []float64{0, 0.5, 1} {
			qv := dp.QuantileValues().AppendEmpty()
			qv.SetQuantile(q)
			qv.SetValue(q * 2)
		}
	})

	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var c otlpdirect.MetricsConverter

		got, dropped, err := c.Convert(data)
		if err != nil {
			return
		}

		if dropped < 0 {
			t.Fatalf("negative dropped count: %d", dropped)
		}

		points := 0

		for i := range got.Resources {
			rm := &got.Resources[i]
			_ = len(rm.Resource.SchemaURL) + len(rm.Resource.Attributes)

			for j := range rm.Scopes {
				sm := &rm.Scopes[j]
				_ = len(sm.Scope.Name)

				for k := range sm.Metrics {
					mt := &sm.Metrics[k]
					_ = len(mt.Name) + len(mt.Unit)

					for p := range mt.Points {
						_ = len(mt.Points[p].Attributes)
						points++
					}
				}
			}
		}

		// The scratch is reused across points and across calls, and the decomposition writes
		// synthesized names into the same arena the attributes come from — so a second pass over
		// the same bytes must produce the same batch, not one built on recycled memory.
		again, againDropped, err := c.Convert(data)
		if err != nil {
			t.Fatalf("second convert of the same input failed: %v", err)
		}

		if againDropped != dropped {
			t.Fatalf("reuse changed the dropped count: %d then %d", dropped, againDropped)
		}

		againPoints := 0
		for i := range again.Resources {
			for j := range again.Resources[i].Scopes {
				for k := range again.Resources[i].Scopes[j].Metrics {
					againPoints += len(again.Resources[i].Scopes[j].Metrics[k].Points)
				}
			}
		}

		if againPoints != points {
			t.Fatalf("reuse changed the point count: %d then %d", points, againPoints)
		}
	})
}
