// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !prometheusremotewrite_prometheus_prompb

package prometheusremotewrite

import (
	"encoding/hex"
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/prompb"
)

// FromTimeSeries converts TimeSeries to OTLP metrics.
func FromTimeSeries(tss []prompb.TimeSeries, settings Settings) (pmetric.Metrics, error) {
	settings.setDefaults()

	var (
		lg            = settings.Logger
		timeThreshold = time.Now().Add(-settings.TimeThreshold)

		pms     = pmetric.NewMetrics()
		res     = pms.ResourceMetrics().AppendEmpty()
		scope   = res.ScopeMetrics().AppendEmpty()
		metrics = scope.Metrics()
	)
	settings.Resource.CopyTo(res.Resource())
	settings.Scope.CopyTo(scope.Scope())

	for _, ts := range tss {
		pm := metrics.AppendEmpty()

		metricName, err := finalName(ts.Labels)
		if err != nil {
			return pms, err
		}
		pm.SetName(metricName)

		s1, s2 := metricSuffixes(metricName)
		var metricsType string
		if s1 != "" || s2 != "" {
			lastSuffixInMetricName := s2
			if IsValidSuffix(lastSuffixInMetricName) {
				metricsType = lastSuffixInMetricName
				if s2 != "" {
					secondSuffixInMetricName := s1
					if IsValidUnit(secondSuffixInMetricName) {
						pm.SetUnit(secondSuffixInMetricName)
					}
				}
			} else if IsValidUnit(lastSuffixInMetricName) {
				pm.SetUnit(lastSuffixInMetricName)
			}
		}

		actualSamples := countPoints(ts.Samples, timeThreshold)
		actualHistograms := countPoints(ts.Histograms, timeThreshold)

		if actualSamples > 0 {
			var slice pmetric.NumberDataPointSlice
			if IsValidCumulativeSuffix(metricsType) {
				pm.SetEmptySum()
				pm.Sum().SetIsMonotonic(true)
				pm.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				slice = pm.Sum().DataPoints()
			} else {
				pm.SetEmptyGauge()
				slice = pm.Gauge().DataPoints()
			}

			for _, s := range ts.Samples {
				timestamp := mapTimestamp(s.Timestamp)
				if ts := timestamp.AsTime(); ts.Before(timeThreshold) {
					lg.Debug("Metric older than the threshold",
						zap.String("metric_name", pm.Name()),
						zap.Time("metric_timestamp", ts),
					)
					continue
				}

				ppoint := slice.AppendEmpty()
				ppoint.SetDoubleValue(s.Value)
				ppoint.SetTimestamp(timestamp)

				attrs := ppoint.Attributes()
				for _, l := range ts.Labels {
					if string(l.Name) == nameStr {
						continue
					}
					attrs.PutStr(string(l.Name), string(l.Value))
				}
				mapExemplars(ts.Exemplars, ppoint.Exemplars())
			}
		} else if actualHistograms > 0 {
			eh := pm.SetEmptyExponentialHistogram()

			for _, h := range ts.Histograms {
				timestamp := mapTimestamp(h.Timestamp)
				if ts := timestamp.AsTime(); ts.Before(timeThreshold) {
					lg.Debug("Metric older than the threshold",
						zap.String("metric_name", pm.Name()),
						zap.Time("metric_timestamp", ts),
					)
					continue
				}

				ppoint := eh.DataPoints().AppendEmpty()
				ppoint.SetTimestamp(timestamp)
				if c, ok := h.Count.AsUint64(); ok {
					ppoint.SetCount(c)
				} else if c, ok := h.Count.AsFloat64(); ok {
					ppoint.SetCount(uint64(c))
				}
				ppoint.SetScale(h.Schema)
				if c, ok := h.ZeroCount.AsUint64(); ok {
					ppoint.SetZeroCount(c)
				} else if c, ok := h.ZeroCount.AsFloat64(); ok {
					ppoint.SetZeroCount(uint64(c))
				}
				mapExpBuckets(promHistorgram{
					deltas: h.NegativeDeltas,
					counts: h.NegativeCounts,
					spans:  h.NegativeSpans,
				}, ppoint.Negative())
				mapExpBuckets(promHistorgram{
					deltas: h.PositiveDeltas,
					counts: h.PositiveCounts,
					spans:  h.PositiveSpans,
				}, ppoint.Positive())
				ppoint.SetSum(h.Sum)
				ppoint.SetZeroThreshold(h.ZeroThreshold)

				attrs := ppoint.Attributes()
				for _, l := range ts.Labels {
					if string(l.Name) == nameStr {
						continue
					}
					attrs.PutStr(string(l.Name), string(l.Value))
				}
				mapExemplars(ts.Exemplars, ppoint.Exemplars())
			}

			if dropped := len(ts.Samples) - actualSamples; dropped > 0 {
				lg.Warn(
					"Some samples are too old and would be dropped",
					zap.Int("received", len(ts.Samples)),
					zap.Int("dropped", dropped),
				)
			}
			if dropped := len(ts.Histograms) - actualHistograms; dropped > 0 {
				lg.Warn(
					"Some histograms are too old and would be dropped",
					zap.Int("received", len(ts.Histograms)),
					zap.Int("dropped", dropped),
				)
			}
		}
	}
	return pms, nil
}

func mapExemplars(exemplars []prompb.Exemplar, slice pmetric.ExemplarSlice) {
	for _, from := range exemplars {
		to := slice.AppendEmpty()
		to.SetDoubleValue(from.Value)
		to.SetTimestamp(mapTimestamp(from.Timestamp))

		attrs := to.FilteredAttributes()
		for _, l := range from.Labels {
			switch string(l.Name) {
			case "span_id":
				var spanID pcommon.SpanID
				if hex.DecodedLen(len(l.Value)) != len(spanID) {
					break
				}
				if _, err := hex.Decode(spanID[:], l.Value); err != nil {
					break
				}
				to.SetSpanID(spanID)
			case "trace_id":
				var traceID pcommon.TraceID
				if hex.DecodedLen(len(l.Value)) != len(traceID) {
					break
				}
				if _, err := hex.Decode(traceID[:], l.Value); err != nil {
					break
				}
				to.SetTraceID(traceID)
			}
			attrs.PutStr(string(l.Name), string(l.Value))
		}
	}
}

func countPoints[
	T any,
	P interface {
		*T
		GetTimestamp() int64
	},
](
	points []T,
	timeThreshold time.Time,
) (samples int) {
	for i := range points {
		unixNano := P(&points[i]).GetTimestamp() * int64(time.Millisecond)
		if time.Unix(0, unixNano).Before(timeThreshold) {
			continue
		}
		samples++
	}
	return samples
}

type promHistorgram struct {
	deltas []int64
	counts []float64
	spans  []prompb.BucketSpan
}

func mapExpBuckets(hist promHistorgram, buckets pmetric.ExponentialHistogramDataPointBuckets) {
	if len(hist.spans) == 0 {
		return
	}
	buckets.SetOffset(hist.spans[0].Offset)

	if counts := buckets.BucketCounts(); len(hist.counts) > 0 {
		for _, c := range hist.counts {
			counts.Append(uint64(c))
		}
	} else {
		var cur float64
		for _, c := range hist.counts {
			cur += c
			counts.Append(uint64(cur))
		}
	}
}

func mapTimestamp(val int64) pcommon.Timestamp {
	t := time.Unix(0, val*int64(time.Millisecond))
	return pcommon.NewTimestampFromTime(t)
}

func finalName(labels []prompb.Label) (ret string, err error) {
	for _, label := range labels {
		if string(label.Name) == nameStr {
			return string(label.Value), nil
		}
	}
	return "", errors.New("label name not found")
}
