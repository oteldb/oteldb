package promrw

import (
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/prompb"
)

// Remote write 2.0 tells the receiver what a series is instead of leaving it to be guessed from the
// name, so ConvertV2 uses the declared type where it is authoritative and falls back to the 1.0
// suffix inference where it is not.
//
// It is authoritative for the types that describe one series: a counter, a gauge, and the
// OpenMetrics info and stateset types, whose value is always 1 and which therefore cannot be
// counters. It is not authoritative for histogram and summary, which describe a *family* — the
// series actually being sent is one of `<name>_bucket`, `<name>_sum` or `<name>_count`, and only
// the suffix says which. Since that is exactly what the 1.0 inference reads, those fall through to
// it and land on the same identity a 1.0 sender would produce for the same series.
//
// The unit is taken from the declared one, which is a Prometheus unit word (`seconds`, `bytes`) —
// the same vocabulary the suffix inference produces, so the two protocols agree on identity rather
// than splitting every series along the way a sender happened to be configured.

// ConvertV2 builds a metrics batch from a remote write 2.0 request, reporting what it did not
// ingest.
//
// The returned batch aliases req and the buffer req was decoded from, under the same contract as
// [Converter.Convert]. A series it cannot store is skipped rather than failing the batch.
func (c *Converter) ConvertV2(req *prompb.WriteRequestV2, o Options) (*metric.Metrics, Counts) {
	o.setDefaults()
	cutoff := o.Now.Add(-o.TimeThreshold).UnixNano()
	sm := c.reset(o)

	var counts Counts
	for i := range req.Timeseries {
		ts := &req.Timeseries[i]
		points := len(ts.Samples) + len(ts.Histograms)

		labels, err := prompb.AppendLabels(c.labels.Alloc(len(ts.LabelsRefs)/2), ts.LabelsRefs, req.Symbols)
		if err != nil {
			counts.Rejected.Invalid += points
			continue
		}

		series, ok := c.series(labels)
		if !ok {
			counts.Rejected.Invalid += points
			continue
		}

		series.kind, ok = kindOfV2(series.name, ts.Metadata, req)
		if !ok {
			// A help or unit ref outside the symbol table means the sender and the receiver disagree
			// about the table, so nothing in the series can be trusted.
			counts.Rejected.Invalid += points
			continue
		}

		appended := c.appendSamplesV2(sm, series, ts.Samples, cutoff)
		counts.Samples += appended
		counts.Rejected.Old += len(ts.Samples) - appended

		// As in 1.0, a series carries samples or histograms, not both.
		if appended > 0 {
			counts.Rejected.Old += len(ts.Histograms)
			continue
		}

		rej := c.appendHistograms(sm, series, ts.Histograms, cutoff)
		counts.Histograms += len(ts.Histograms) - rej.Total()
		counts.Rejected.add(rej)
	}

	return &c.batch, counts
}

// appendSamplesV2 appends the series' in-window samples, carrying over the start timestamp 1.0 had
// no way to express.
func (c *Converter) appendSamplesV2(
	sm *metric.ScopeMetrics,
	s series,
	samples []prompb.SampleV2,
	cutoff int64,
) (appended int) {
	var mt *metric.Metric
	for _, sample := range samples {
		tsNano := msToNano(sample.Timestamp)
		if tsNano < cutoff {
			continue
		}

		if mt == nil {
			mt = c.addMetric(sm, s)
		}
		p := mt.AddPoint()
		p.Attributes = s.attrs
		p.StartTs = msToNano(sample.StartTimestamp)
		p.Ts = tsNano
		p.Value = sample.Value
		appended++
	}

	return appended
}

// kindOfV2 resolves a series' kind from its declared metadata, falling back to the name suffix
// where the declared type describes a family rather than one series. It reports false when a
// metadata ref is outside the request's symbol table.
func kindOfV2(name []byte, md prompb.Metadata, req *prompb.WriteRequestV2) (kind, bool) {
	unit, ok := req.Symbol(md.UnitRef)
	if !ok {
		return kind{}, false
	}
	if _, ok := req.Symbol(md.HelpRef); !ok {
		return kind{}, false
	}

	k := kindOf(name)
	if len(unit) > 0 {
		k.unit = unit
	}

	switch md.Type {
	case prompb.MetricTypeCounter:
		k.cumulative, k.monotonic = true, true
	case prompb.MetricTypeGauge:
		k.cumulative, k.monotonic = false, false
	case prompb.MetricTypeInfo, prompb.MetricTypeStateSet:
		// Their value is always 1, so they accumulate without ever counting up.
		k.cumulative, k.monotonic = true, false
	default:
		// Unspecified, histogram, gaugehistogram and summary: the suffix decides, exactly as it
		// does for a 1.0 sender.
	}

	return k, true
}
