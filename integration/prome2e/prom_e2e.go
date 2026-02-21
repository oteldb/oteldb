// Package prome2e provides scripts for E2E testing Prometheus API implementation.
package prome2e

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// BatchSet is a set of batches.
type BatchSet struct {
	Batches []pmetric.Metrics
	Labels  map[string]map[string]struct{}
	Series  []map[string]string

	Start pcommon.Timestamp
	End   pcommon.Timestamp
}

// ParseBatchSet parses JSON batches from given reader.
func ParseBatchSet(r io.Reader) (s BatchSet, _ error) {
	d := jx.Decode(r, 4096)
	u := pmetric.JSONUnmarshaler{}

	for d.Next() != jx.Invalid {
		data, err := d.Raw()
		if err != nil {
			return s, errors.Wrap(err, "read line")
		}

		raw, err := u.UnmarshalMetrics(data)
		if err != nil {
			return s, errors.Wrap(err, "parse batch")
		}

		if err := s.addBatch(raw); err != nil {
			return s, errors.Wrap(err, "add batch")
		}
	}
	return s, nil
}

// MatchingSeries returns series that match given label matchers.
func (s *BatchSet) MatchingSeries(match []string) (r []map[string]string, _ error) {
	matcherSets := make([][]*labels.Matcher, 0, len(match))
	for _, s := range match {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, errors.Wrapf(err, "parse metric selector %q", s)
		}
		matcherSets = append(matcherSets, matchers)
	}

	for _, series := range s.Series {
		for _, set := range matcherSets {
			if matchesSeries(series, set) {
				r = append(r, series)
				break
			}
		}
	}
	return r, nil
}

func matchesSeries(series map[string]string, set []*labels.Matcher) bool {
	for _, m := range set {
		v, ok := series[m.Name]
		if !ok || !m.Matches(v) {
			return false
		}
	}
	return true
}

func (s *BatchSet) addBatch(raw pmetric.Metrics) error {
	s.Batches = append(s.Batches, raw)

	resMetrics := raw.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		resLog := resMetrics.At(i)
		res := resLog.Resource()

		scopeMetrics := resLog.ScopeMetrics()
		for i := 0; i < scopeMetrics.Len(); i++ {
			scopeMetrics := scopeMetrics.At(i)
			scope := scopeMetrics.Scope()

			metrics := scopeMetrics.Metrics()
			for i := 0; i < metrics.Len(); i++ {
				metric := metrics.At(i)
				if err := s.addMetric(res.Attributes(), scope.Attributes(), metric); err != nil {
					return errors.Wrap(err, "add metric")
				}
			}
		}
	}
	return nil
}

func (s *BatchSet) addMetric(res, scope pcommon.Map, metric pmetric.Metric) error {
	switch t := metric.Type(); t {
	case pmetric.MetricTypeGauge:
		points := metric.Gauge().DataPoints()
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)

			s.addTimestamp(point.Timestamp())
			s.addSeries(metric.Name(), res, scope, point.Attributes())
		}
		return nil
	case pmetric.MetricTypeSum:
		points := metric.Sum().DataPoints()
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)

			s.addTimestamp(point.Timestamp())
			s.addSeries(metric.Name(), res, scope, point.Attributes())
		}
		return nil
	case pmetric.MetricTypeHistogram:
		suffixes := []string{
			"_count",
			"_bucket",
		}

		points := metric.Histogram().DataPoints()
		if points.Len() == 0 {
			name := metric.Name()
			attrs := pcommon.NewMap()
			for _, suffix := range suffixes {
				s.addSeries(name+suffix, res, scope, attrs)
			}
		}
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)
			attrs := point.Attributes()

			if point.HasSum() {
				s.addSeries(metric.Name()+"_sum", res, scope, attrs)
			}
			if point.HasMin() {
				s.addSeries(metric.Name()+"_min", res, scope, attrs)
			}
			if point.HasMax() {
				s.addSeries(metric.Name()+"_max", res, scope, attrs)
			}

			s.addTimestamp(point.Timestamp())
			// NOTE: histogram names are not saved as-is.
			name := metric.Name()
			for _, suffix := range suffixes {
				s.addSeries(name+suffix, res, scope, attrs)
			}
		}
		return nil
	case pmetric.MetricTypeExponentialHistogram:
		points := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)

			s.addTimestamp(point.Timestamp())
			s.addSeries(metric.Name(), res, scope, point.Attributes())
		}
		return nil
	case pmetric.MetricTypeSummary:
		suffixes := []string{
			"_count",
			"_sum",
		}

		points := metric.Summary().DataPoints()
		if points.Len() == 0 {
			name := metric.Name()
			attrs := pcommon.NewMap()
			s.addSeries(name, res, scope, attrs)
			for _, suffix := range suffixes {
				s.addSeries(name+suffix, res, scope, attrs)
			}
		}
		for i := 0; i < points.Len(); i++ {
			point := points.At(i)
			attrs := point.Attributes()

			s.addTimestamp(point.Timestamp())
			name := metric.Name()
			s.addSeries(name, res, scope, attrs)
			for _, suffix := range suffixes {
				s.addSeries(name+suffix, res, scope, attrs)
			}
		}
		return nil
	case pmetric.MetricTypeEmpty:
		return nil
	default:
		return errors.Errorf("unexpected type %v", t)
	}
}

func (s *BatchSet) addSeries(name string, res, scope, attrs pcommon.Map) {
	s.addLabel(labels.MetricName, name)

	lb := map[string]string{
		labels.MetricName: name,
	}
	for _, m := range []pcommon.Map{
		res,
		scope,
		attrs,
	} {
		m.Range(func(k string, v pcommon.Value) bool {
			switch t := v.Type(); t {
			case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			default:
				val := v.AsString()

				s.addLabel(k, val)
				lb[k] = val
			}
			return true
		})
	}

	s.Series = append(s.Series, lb)
}

func (s *BatchSet) addTimestamp(ts pcommon.Timestamp) {
	if s.Start == 0 || ts < s.Start {
		s.Start = ts
	}
	if ts > s.End {
		s.End = ts
	}
}

func (s *BatchSet) addLabel(label, val string) {
	if s.Labels == nil {
		s.Labels = map[string]map[string]struct{}{}
	}
	m := s.Labels[label]
	if m == nil {
		m = map[string]struct{}{}
		s.Labels[label] = m
	}
	m[val] = struct{}{}
}
