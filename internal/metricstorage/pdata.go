package metricstorage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// NumberPoint is a single decomposed numeric sample as chstorage stores it: one point of one
// Prometheus-style series (a gauge/sum, or a histogram/summary component whose name already
// carries the _count/_sum/_bucket suffix and whose Attrs already carry the le/quantile label).
type NumberPoint struct {
	Name        string
	Unit        string
	Description string

	Resource otelstorage.Attrs
	Scope    otelstorage.Attrs
	Attrs    otelstorage.Attrs

	Timestamp pcommon.Timestamp
	Value     float64
}

// ExpHistogramPoint is a single exponential-histogram datapoint as chstorage stores it (native,
// not decomposed), resolved together with its series metadata.
type ExpHistogramPoint struct {
	Name        string
	Unit        string
	Description string

	Resource otelstorage.Attrs
	Scope    otelstorage.Attrs
	Attrs    otelstorage.Attrs

	Timestamp pcommon.Timestamp

	Count     uint64
	Sum       *float64
	Min       *float64
	Max       *float64
	Scale     int32
	ZeroCount uint64

	PositiveOffset       int32
	PositiveBucketCounts []uint64
	NegativeOffset       int32
	NegativeBucketCounts []uint64

	Flags uint32
}

// scopeKey groups datapoints into ResourceMetrics/ScopeMetrics by resource and scope identity.
// chstorage does not store scope name/version separately for metrics (only a scope attribute
// map), so grouping is by attribute-set hashes alone.
type scopeKey struct {
	resource otelstorage.Hash
	scope    otelstorage.Hash
}

// metricsGrouper accumulates a pmetric.Metrics, opening one ResourceMetrics/ScopeMetrics per
// distinct (resource, scope) and one Metric per name within a scope.
type metricsGrouper struct {
	md     pmetric.Metrics
	scopes map[scopeKey]pmetric.ScopeMetrics
	// metrics keys a Metric by its scope pointer identity and name, so datapoints of the same
	// series accumulate under a single Metric.
	metrics map[metricKey]pmetric.Metric
}

type metricKey struct {
	scope scopeKey
	name  string
}

func newMetricsGrouper() *metricsGrouper {
	return &metricsGrouper{
		md:      pmetric.NewMetrics(),
		scopes:  map[scopeKey]pmetric.ScopeMetrics{},
		metrics: map[metricKey]pmetric.Metric{},
	}
}

func (g *metricsGrouper) scopeMetrics(resource, scope otelstorage.Attrs) (scopeKey, pmetric.ScopeMetrics) {
	key := scopeKey{resource: resource.Hash(), scope: scope.Hash()}
	sm, ok := g.scopes[key]
	if !ok {
		rm := g.md.ResourceMetrics().AppendEmpty()
		resource.CopyTo(rm.Resource().Attributes())
		sm = rm.ScopeMetrics().AppendEmpty()
		scope.CopyTo(sm.Scope().Attributes())
		g.scopes[key] = sm
	}
	return key, sm
}

// metric returns the Metric for the given series, creating it (via newMetric) on first use so
// per-name datapoints accumulate together.
func (g *metricsGrouper) metric(resource, scope otelstorage.Attrs, name string, newMetric func(pmetric.ScopeMetrics) pmetric.Metric) pmetric.Metric {
	skey, sm := g.scopeMetrics(resource, scope)
	mkey := metricKey{scope: skey, name: name}
	m, ok := g.metrics[mkey]
	if !ok {
		m = newMetric(sm)
		g.metrics[mkey] = m
	}
	return m
}

// NumberPointsToMetrics converts decomposed numeric samples back into an OTLP [pmetric.Metrics]
// payload, one Gauge per (resource, scope, name). chstorage discards the original gauge-vs-sum
// distinction for these points, so every series is emitted as a gauge; the values, names, and
// labels round-trip exactly.
func NumberPointsToMetrics(points []NumberPoint) pmetric.Metrics {
	g := newMetricsGrouper()
	for _, p := range points {
		m := g.metric(p.Resource, p.Scope, p.Name, func(sm pmetric.ScopeMetrics) pmetric.Metric {
			m := sm.Metrics().AppendEmpty()
			m.SetName(p.Name)
			m.SetUnit(p.Unit)
			m.SetDescription(p.Description)
			m.SetEmptyGauge()
			return m
		})
		dp := m.Gauge().DataPoints().AppendEmpty()
		p.Attrs.CopyTo(dp.Attributes())
		dp.SetTimestamp(p.Timestamp)
		dp.SetDoubleValue(p.Value)
	}
	return g.md
}

// ExpHistogramsToMetrics converts exponential-histogram datapoints back into an OTLP
// [pmetric.Metrics] payload, one ExponentialHistogram per (resource, scope, name). Temporality is
// set to cumulative (chstorage does not store it, and cumulative is the norm for stored series).
func ExpHistogramsToMetrics(points []ExpHistogramPoint) pmetric.Metrics {
	g := newMetricsGrouper()
	for _, p := range points {
		m := g.metric(p.Resource, p.Scope, p.Name, func(sm pmetric.ScopeMetrics) pmetric.Metric {
			m := sm.Metrics().AppendEmpty()
			m.SetName(p.Name)
			m.SetUnit(p.Unit)
			m.SetDescription(p.Description)
			eh := m.SetEmptyExponentialHistogram()
			eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			return m
		})
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		p.Attrs.CopyTo(dp.Attributes())
		dp.SetTimestamp(p.Timestamp)
		dp.SetFlags(pmetric.DataPointFlags(p.Flags))
		dp.SetCount(p.Count)
		if p.Sum != nil {
			dp.SetSum(*p.Sum)
		}
		if p.Min != nil {
			dp.SetMin(*p.Min)
		}
		if p.Max != nil {
			dp.SetMax(*p.Max)
		}
		dp.SetScale(p.Scale)
		dp.SetZeroCount(p.ZeroCount)
		dp.Positive().SetOffset(p.PositiveOffset)
		dp.Positive().BucketCounts().FromRaw(p.PositiveBucketCounts)
		dp.Negative().SetOffset(p.NegativeOffset)
		dp.Negative().BucketCounts().FromRaw(p.NegativeBucketCounts)
	}
	return g.md
}
