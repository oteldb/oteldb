// Package promrw converts Prometheus remote write requests directly into the storage engine's
// metrics ingest batch.
//
// It replaces the receiver → pdata → pdataconv path with a single pass over the decoded
// [prompb.WriteRequest]: label names and values are handed to the engine as sub-slices of the
// decode buffer, never copied into Go strings and never materialized as pdata. The engine copies
// what it retains (record cells into its column blobs, identity into its symbol table), so the
// decode buffer may be recycled once the write returns — see [Converter.Convert].
package promrw

import (
	"bytes"
	"time"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/xarena"
)

// nameLabel is the Prometheus metric name label. It is the metric's name, not one of its
// attributes.
var nameLabel = []byte("__name__")

// Options configures a [Converter.Convert] call.
type Options struct {
	// TimeThreshold drops points older than it. Zero means 24h, matching the receiver default.
	TimeThreshold time.Duration
	// Resource and Scope are the identity every converted series is ingested under. Remote write
	// carries neither, so they are the receiver's own (typically empty).
	Resource signal.Resource
	Scope    signal.Scope
	// Now overrides the clock the TimeThreshold cutoff is computed from. Zero means time.Now.
	Now time.Time
}

func (o *Options) setDefaults() {
	if o.TimeThreshold == 0 {
		o.TimeThreshold = 24 * time.Hour
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
}

// Converter converts remote write requests into [metric.Metrics]. It retains the batch and the
// scratch it is built from, so a converter reused across requests allocates nothing in steady
// state. It is not safe for concurrent use; pool one per in-flight request.
type Converter struct {
	batch  metric.Metrics
	attrs  xarena.Arena[signal.KeyValue]
	names  xarena.Arena[byte]
	labels xarena.Arena[prompb.Label]
}

// Convert builds a metrics batch from the request's timeseries, reporting what it did not ingest.
//
// The returned batch aliases both tss and the buffer tss was decoded from: it is valid until the
// next Convert call on this converter, and the decode buffer must outlive the write it is passed
// to.
//
// A series it cannot store is skipped rather than failing the batch — see [Rejected].
func (c *Converter) Convert(tss []prompb.TimeSeries, o Options) (*metric.Metrics, Counts) {
	o.setDefaults()
	cutoff := o.Now.Add(-o.TimeThreshold).UnixNano()
	sm := c.reset(o)

	var counts Counts
	for i := range tss {
		ts := &tss[i]

		series, ok := c.series(ts.Labels)
		if !ok {
			counts.Rejected.Invalid += len(ts.Samples) + len(ts.Histograms)
			continue
		}
		// Remote write 1.0 carries no type information, so the name suffix is all there is to go on.
		series.kind = kindOf(series.name)

		c.appendSeries(sm, series, ts.Samples, ts.Histograms, cutoff, &counts)
	}

	return &c.batch, counts
}

// reset readies the converter for one request and returns the single scope every series is ingested
// under.
func (c *Converter) reset(o Options) *metric.ScopeMetrics {
	c.batch.Reset()
	c.attrs.Reset()
	c.names.Reset()
	c.labels.Reset()

	rm := c.batch.AddResource()
	rm.Resource = o.Resource
	sm := rm.AddScope()
	sm.Scope = o.Scope

	return sm
}

// series is one timeseries' resolved identity: its metric name, the attributes shared by every
// point, and the metric header those points are stored under.
type series struct {
	name  []byte
	attrs signal.Attributes
	kind  kind
}

// series validates and resolves a timeseries' labels. It reports false for a label set that cannot
// be stored.
func (c *Converter) series(labels []prompb.Label) (series, bool) {
	if !validLabels(labels) {
		return series{}, false
	}

	name, _ := metricName(labels)
	attrs := c.labelAttrs(labels)
	if hasDuplicateKey(attrs) {
		return series{}, false
	}

	return series{name: name, attrs: attrs}, true
}

// appendSeries stores one series' samples, or its histograms when it has no in-window sample.
func (c *Converter) appendSeries(
	sm *metric.ScopeMetrics,
	s series,
	samples []prompb.Sample,
	histograms []prompb.Histogram,
	cutoff int64,
	counts *Counts,
) {
	appended := c.appendSamples(sm, s, samples, cutoff)
	counts.Samples += appended
	counts.Rejected.Old += len(samples) - appended

	// Prometheus sends a series as either float samples or native histograms, never both;
	// histograms are decomposed only when the series carries no in-window sample.
	if appended > 0 {
		counts.Rejected.Old += len(histograms)
		return
	}

	rej := c.appendHistograms(sm, s, histograms, cutoff)
	counts.Histograms += len(histograms) - rej.Total()
	counts.Rejected.add(rej)
}

// appendSamples appends the series' in-window float samples as one gauge or sum metric, returning
// how many were appended. Nothing is appended when every sample is out of window.
func (c *Converter) appendSamples(
	sm *metric.ScopeMetrics,
	s series,
	samples []prompb.Sample,
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
		p.Ts = tsNano
		p.Value = sample.Value
		appended++
	}
	return appended
}

// addMetric appends the metric header a series' points are stored under.
func (c *Converter) addMetric(sm *metric.ScopeMetrics, s series) *metric.Metric {
	mt := sm.AddMetric()
	mt.Name = s.name
	mt.Unit = s.kind.unit
	if s.kind.cumulative {
		mt.Kind = metric.KindSum
		mt.Temporality = metric.TemporalityCumulative
		mt.Monotonic = s.kind.monotonic
	} else {
		mt.Kind = metric.KindGauge
	}

	return mt
}

// kind is the metric-header shape a series is stored under, which contributes to its identity.
type kind struct {
	unit       []byte
	cumulative bool
	monotonic  bool
}

// kindOf infers a series' kind from its name suffixes, which is all remote write 1.0 carries.
func kindOf(name []byte) kind {
	unit, cumulative := classify(name)

	return kind{unit: unit, cumulative: cumulative, monotonic: cumulative}
}

// labelAttrs converts the series' labels (all but __name__) into one sorted attribute set shared
// by every point of the series.
func (c *Converter) labelAttrs(labels []prompb.Label) signal.Attributes {
	kvs := c.attrs.Alloc(len(labels))
	for _, l := range labels {
		if bytes.Equal(l.Name, nameLabel) {
			continue
		}
		kvs = append(kvs, signal.KeyValue{Key: l.Name, Value: signal.StringValue(l.Value)})
	}
	return signal.NewAttributes(kvs...)
}

// withLabel returns attrs plus one string label, re-sorted.
func (c *Converter) withLabel(attrs signal.Attributes, key, value []byte) signal.Attributes {
	kvs := c.attrs.Alloc(len(attrs) + 1)
	kvs = append(kvs, attrs...)
	kvs = append(kvs, signal.KeyValue{Key: key, Value: signal.StringValue(value)})
	return signal.NewAttributes(kvs...)
}

// suffixed returns name+suffix, carved from the converter's name arena.
func (c *Converter) suffixed(name []byte, suffix string) []byte {
	return c.names.Concat(name, []byte(suffix))
}

func metricName(labels []prompb.Label) ([]byte, bool) {
	for _, l := range labels {
		if bytes.Equal(l.Name, nameLabel) {
			return l.Value, true
		}
	}
	return nil, false
}

func msToNano(ms int64) int64 { return ms * int64(time.Millisecond) }

// classify infers a series' unit and whether it is a cumulative counter from its name suffixes,
// mirroring what the pdata translator inferred before ingest.
func classify(name []byte) (unit []byte, cumulative bool) {
	s1, s2 := suffixes(name)
	switch {
	case isCounterSuffix(s2):
		if isUnit(s1) {
			unit = s1
		}
		return unit, isCumulativeSuffix(s2)
	case isUnit(s2):
		return s2, false
	default:
		return nil, false
	}
}

// suffixes returns the last two underscore-separated components of name: s2 is the last, s1 the
// one before it, nil when the name has only one component.
//
// Both are needed because the unit sits before the type: in `http_duration_seconds_sum` the type
// is `sum` and the unit `seconds`. A name with a single separator still has a type — `foo_total`
// is a counter — so requiring two separators, as this used to, silently made every such series a
// gauge while the same name with one more component became a sum.
func suffixes(name []byte) (s1, s2 []byte) {
	i := bytes.LastIndexByte(name, '_')
	if i < 0 {
		return nil, nil
	}
	return lastComponent(name[:i]), name[i+1:]
}

// lastComponent returns the last underscore-separated component of s, which is s itself when s has
// no separator: in `seconds_total` the unit is the whole head.
func lastComponent(s []byte) []byte {
	if i := bytes.LastIndexByte(s, '_'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func isCounterSuffix(s []byte) bool {
	switch string(s) {
	case "max", "sum", "count", "total":
		return true
	default:
		return false
	}
}

func isCumulativeSuffix(s []byte) bool {
	switch string(s) {
	case "sum", "count", "total":
		return true
	default:
		return false
	}
}

func isUnit(s []byte) bool {
	switch string(s) {
	case "seconds", "bytes":
		return true
	default:
		return false
	}
}
