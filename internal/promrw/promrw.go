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

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/xarena"
)

// nameLabel is the Prometheus metric name label. It is the metric's name, not one of its
// attributes.
var nameLabel = []byte("__name__")

// ErrNoName is returned for a timeseries carrying no __name__ label.
var ErrNoName = errors.New("timeseries has no __name__ label")

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
	batch metric.Metrics
	attrs xarena.Arena[signal.KeyValue]
	names xarena.Arena[byte]
}

// Convert builds a metrics batch from the request's timeseries.
//
// The returned batch aliases both tss and the buffer tss was decoded from: it is valid until the
// next Convert call on this converter, and the decode buffer must outlive the write it is passed
// to. Points older than [Options.TimeThreshold] are dropped and counted in dropped.
func (c *Converter) Convert(tss []prompb.TimeSeries, o Options) (_ *metric.Metrics, dropped int, _ error) {
	o.setDefaults()
	cutoff := o.Now.Add(-o.TimeThreshold).UnixNano()

	c.batch.Reset()
	c.attrs.Reset()
	c.names.Reset()

	rm := c.batch.AddResource()
	rm.Resource = o.Resource
	sm := rm.AddScope()
	sm.Scope = o.Scope

	for i := range tss {
		ts := &tss[i]

		name, ok := metricName(ts.Labels)
		if !ok {
			return nil, dropped, errors.Wrapf(ErrNoName, "timeseries %d", i)
		}
		attrs := c.labelAttrs(ts.Labels)

		samples := c.appendSamples(sm, ts, name, attrs, cutoff)
		dropped += len(ts.Samples) - samples

		// Prometheus sends a series as either float samples or native histograms, never both;
		// histograms are decomposed only when the series carries no in-window sample.
		if samples == 0 {
			dropped += c.appendHistograms(sm, ts, name, attrs, cutoff)
		} else {
			dropped += len(ts.Histograms)
		}
	}

	return &c.batch, dropped, nil
}

// appendSamples appends the series' in-window float samples as one gauge or sum metric, returning
// how many were appended. Nothing is appended when every sample is out of window.
func (c *Converter) appendSamples(
	sm *metric.ScopeMetrics,
	ts *prompb.TimeSeries,
	name []byte,
	attrs signal.Attributes,
	cutoff int64,
) (appended int) {
	var mt *metric.Metric
	for _, s := range ts.Samples {
		tsNano := msToNano(s.Timestamp)
		if tsNano < cutoff {
			continue
		}

		if mt == nil {
			mt = c.addMetric(sm, name)
		}
		p := mt.AddPoint()
		p.Attributes = attrs
		p.Ts = tsNano
		p.Value = s.Value
		appended++
	}
	return appended
}

// addMetric appends the metric a remote write series maps to: its unit and kind are inferred from
// the name suffix, the only type information remote write carries.
func (c *Converter) addMetric(sm *metric.ScopeMetrics, name []byte) *metric.Metric {
	mt := sm.AddMetric()
	mt.Name = name

	unit, cumulative := classify(name)
	mt.Unit = unit
	if cumulative {
		mt.Kind = metric.KindSum
		mt.Temporality = metric.TemporalityCumulative
		mt.Monotonic = true
	} else {
		mt.Kind = metric.KindGauge
	}
	return mt
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
