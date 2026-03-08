package oteldbexporter

import (
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// sampleExemplars removes exemplars that the sampler decides to drop.
func sampleExemplars(md pmetric.Metrics, cfg SamplingConfig) {
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
						dps.At(n).Exemplars().RemoveIf(func(pmetric.Exemplar) bool { return !cfg.keep() })
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						dps.At(n).Exemplars().RemoveIf(func(pmetric.Exemplar) bool { return !cfg.keep() })
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						dps.At(n).Exemplars().RemoveIf(func(pmetric.Exemplar) bool { return !cfg.keep() })
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for n := 0; n < dps.Len(); n++ {
						dps.At(n).Exemplars().RemoveIf(func(pmetric.Exemplar) bool { return !cfg.keep() })
					}
				}
			}
		}
	}
}
