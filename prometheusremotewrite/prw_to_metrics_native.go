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

//go:build prometheusremotewrite_prometheus_prompb

package prometheusremotewrite

import (
	"encoding/hex"
	"errors"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// FromTimeSeries converts TimeSeries to OTLP metrics.
func FromTimeSeries(tss []prompb.TimeSeries, settings Settings) (pmetric.Metrics, error) {
	var (
		pms           = pmetric.NewMetrics()
		timeThreshold = time.Now().Add(-time.Duration(settings.TimeThreshold) * time.Hour)
	)
	for _, ts := range tss {
		pm := pms.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()

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

		if countSamples(ts, timeThreshold) != 0 {
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
				ppoint := slice.AppendEmpty()
				ppoint.SetDoubleValue(s.Value)
				ppoint.SetTimestamp(mapTimestamp(s.Timestamp))
				if ppoint.Timestamp().AsTime().Before(timeThreshold) {
					settings.Logger.Debug("Metric older than the threshold",
						zap.String("metric name", pm.Name()),
						zap.Time("metric_timestamp", ppoint.Timestamp().AsTime()),
					)
					continue
				}

				attrs := ppoint.Attributes()
				for _, l := range ts.Labels {
					labelName := l.Name
					if l.Name == nameStr {
						continue
					}
					attrs.PutStr(labelName, l.Value)
				}
				mapExemplars(ts.Exemplars, ppoint.Exemplars())
			}
		}
	}
	return pms, nil
}

func countSamples(ts prompb.TimeSeries, timeThreshold time.Time) (samples int) {
	for _, s := range ts.Samples {
		ts := time.Unix(0, s.Timestamp*int64(time.Millisecond))
		if ts.Before(timeThreshold) {
			continue
		}
		samples++
	}
	return samples
}

func mapExemplars(exemplars []prompb.Exemplar, slice pmetric.ExemplarSlice) {
	for _, from := range exemplars {
		to := slice.AppendEmpty()
		to.SetDoubleValue(from.Value)
		to.SetTimestamp(mapTimestamp(from.Timestamp))

		attrs := to.FilteredAttributes()
		for _, l := range from.Labels {
			switch l.Name {
			case "span_id":
				var spanID pcommon.SpanID
				if hex.DecodedLen(len(l.Value)) != len(spanID) {
					break
				}
				if _, err := hex.Decode(spanID[:], []byte(l.Value)); err != nil {
					break
				}
				to.SetSpanID(spanID)
			case "trace_id":
				var traceID pcommon.TraceID
				if hex.DecodedLen(len(l.Value)) != len(traceID) {
					break
				}
				if _, err := hex.Decode(traceID[:], []byte(l.Value)); err != nil {
					break
				}
				to.SetTraceID(traceID)
			}
			attrs.PutStr(l.Name, l.Value)
		}
	}
}

func mapTimestamp(val int64) pcommon.Timestamp {
	t := time.Unix(0, val*int64(time.Millisecond))
	return pcommon.NewTimestampFromTime(t)
}

func finalName(labels []prompb.Label) (ret string, err error) {
	for _, label := range labels {
		if label.Name == nameStr {
			return label.Value, nil
		}
	}
	return "", errors.New("label name not found")
}
