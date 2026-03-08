package oteldbexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// makeMetricsWithExemplars builds one ResourceMetrics → one ScopeMetrics with
// one data point per metric type (Gauge, Sum, Histogram, ExponentialHistogram),
// each containing exemplarCount exemplars.
func makeMetricsWithExemplars(exemplarCount int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	addExemplars := func(es pmetric.ExemplarSlice) {
		for range exemplarCount {
			es.AppendEmpty()
		}
	}

	g := sm.Metrics().AppendEmpty()
	g.SetName("gauge")
	addExemplars(g.SetEmptyGauge().DataPoints().AppendEmpty().Exemplars())

	s := sm.Metrics().AppendEmpty()
	s.SetName("sum")
	addExemplars(s.SetEmptySum().DataPoints().AppendEmpty().Exemplars())

	h := sm.Metrics().AppendEmpty()
	h.SetName("histogram")
	addExemplars(h.SetEmptyHistogram().DataPoints().AppendEmpty().Exemplars())

	eh := sm.Metrics().AppendEmpty()
	eh.SetName("exp_histogram")
	addExemplars(eh.SetEmptyExponentialHistogram().DataPoints().AppendEmpty().Exemplars())

	return md
}

func countExemplars(md pmetric.Metrics) int {
	total := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						total += dps.At(n).Exemplars().Len()
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						total += dps.At(n).Exemplars().Len()
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						total += dps.At(n).Exemplars().Len()
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						total += dps.At(n).Exemplars().Len()
					}
				}
			}
		}
	}
	return total
}

func TestSampleExemplars_DropAll(t *testing.T) {
	const exemplarsPerType = 5
	md := makeMetricsWithExemplars(exemplarsPerType)
	// 4 metric types × 5 exemplars each.
	require.Equal(t, 4*exemplarsPerType, countExemplars(md))
	// Metric data points must survive.
	require.Equal(t, 4, md.MetricCount())

	sampleExemplars(md, SamplingConfig{Drop: true})

	require.Equal(t, 0, countExemplars(md))
	// Data points are preserved; only exemplars are removed.
	require.Equal(t, 4, md.MetricCount())
}

func TestSampleExemplars_Disabled(t *testing.T) {
	const exemplarsPerType = 5
	md := makeMetricsWithExemplars(exemplarsPerType)
	sampleExemplars(md, SamplingConfig{})
	require.Equal(t, 4*exemplarsPerType, countExemplars(md))
}

func TestSampleExemplars_RateOne(t *testing.T) {
	const exemplarsPerType = 5
	md := makeMetricsWithExemplars(exemplarsPerType)
	sampleExemplars(md, SamplingConfig{Rate: 1.0})
	require.Equal(t, 4*exemplarsPerType, countExemplars(md))
}

func TestSampleExemplars_RateSampling(t *testing.T) {
	const exemplarsPerType = 500
	md := makeMetricsWithExemplars(exemplarsPerType)
	sampleExemplars(md, SamplingConfig{Rate: 0.5})
	got := countExemplars(md)
	total := 4 * exemplarsPerType
	// Expect ~50%, allow ±15%.
	require.InDelta(t, total/2, got, float64(total)*0.15)
}

func TestSampleExemplars_AllMetricTypes(t *testing.T) {
	// Verify each metric type is handled: after Drop, all types have 0 exemplars
	// but still have their data point.
	md := makeMetricsWithExemplars(3)
	sampleExemplars(md, SamplingConfig{Drop: true})

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		var exemplarsLen int
		switch m.Type() {
		case pmetric.MetricTypeGauge:
			exemplarsLen = m.Gauge().DataPoints().At(0).Exemplars().Len()
		case pmetric.MetricTypeSum:
			exemplarsLen = m.Sum().DataPoints().At(0).Exemplars().Len()
		case pmetric.MetricTypeHistogram:
			exemplarsLen = m.Histogram().DataPoints().At(0).Exemplars().Len()
		case pmetric.MetricTypeExponentialHistogram:
			exemplarsLen = m.ExponentialHistogram().DataPoints().At(0).Exemplars().Len()
		}
		require.Equal(t, 0, exemplarsLen, "metric %q should have no exemplars", m.Name())
	}
}
