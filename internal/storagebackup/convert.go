package storagebackup

import (
	"bytes"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// Everything below deep-copies out of the chunk. A chunk read from a file aliases the reader's
// frame buffer, which the next chunk overwrites, while the batch being built outlives many chunks.

// appendLogs converts one chunk into a log stream appended to dst, returning the record count.
func appendLogs(dst *siglog.Logs, c *Chunk) int {
	cols := recordColumns(c)

	sl := addStream(dst.AddResource(), c.Series)
	for i := range c.Timestamps {
		r := sl.AddRecord()
		r.Timestamp = c.Timestamps[i]
		r.ObservedTimestamp = cols.int(siglog.ColObserved, i)
		r.SeverityNumber = int32(cols.int(siglog.ColSeverity, i))
		r.Flags = uint32(cols.int(siglog.ColFlags, i))
		r.Dropped = uint32(cols.int(siglog.ColDropped, i))
		r.SeverityText = cols.bytes(siglog.ColSeverityText, i)
		r.Body = cols.bytes(siglog.ColBody, i)
		r.TraceID = cols.bytes(siglog.ColTraceID, i)
		r.SpanID = cols.bytes(siglog.ColSpanID, i)
		r.Attributes = cols.attrs(siglog.ColAttrs, i)
	}
	return len(c.Timestamps)
}

// appendTraces converts one chunk into a span stream appended to dst, returning the span count.
//
// The nested-set columns are not restored: they are derived per write batch by the traces
// projector, so writing them back would be writing a value the destination is about to recompute.
func appendTraces(dst *sigtrace.Traces, c *Chunk) int {
	cols := recordColumns(c)

	ss := addSpanStream(dst.AddResource(), c.Series)
	for i := range c.Timestamps {
		sp := ss.AddSpan()
		sp.Start = c.Timestamps[i]
		sp.End = sp.Start + cols.int(sigtrace.ColDuration, i)
		sp.Kind = int32(cols.int(sigtrace.ColKind, i))
		sp.StatusCode = int32(cols.int(sigtrace.ColStatusCode, i))
		sp.TraceID = cols.bytes(sigtrace.ColTraceID, i)
		sp.SpanID = cols.bytes(sigtrace.ColSpanID, i)
		sp.ParentSpanID = cols.bytes(sigtrace.ColParentSpanID, i)
		sp.Name = cols.bytes(sigtrace.ColName, i)
		sp.StatusMessage = cols.bytes(sigtrace.ColStatusMsg, i)
		sp.Attributes = cols.attrs(sigtrace.ColAttrs, i)

		if raw := cols.raw(sigtrace.ColEvents, i); len(raw) > 0 {
			if evs, err := sigtrace.DecodeEvents(raw); err == nil {
				for _, ev := range evs {
					e := sp.AddEvent()
					e.Time = ev.Time
					e.Name = bytes.Clone(ev.Name)
					e.Dropped = ev.Dropped
					e.Attributes = ev.Attributes.Clone()
				}
			}
		}
		if raw := cols.raw(sigtrace.ColLinks, i); len(raw) > 0 {
			if links, err := sigtrace.DecodeLinks(raw); err == nil {
				for _, ln := range links {
					l := sp.AddLink()
					l.TraceID = bytes.Clone(ln.TraceID)
					l.SpanID = bytes.Clone(ln.SpanID)
					l.TraceState = bytes.Clone(ln.TraceState)
					l.Dropped = ln.Dropped
					l.Attributes = ln.Attributes.Clone()
				}
			}
		}
	}
	return len(c.Timestamps)
}

// appendMetrics converts one chunk into a metric series appended to dst, returning the sample
// count. The chunk's identity carries the metric-specific fields as reserved labels, which
// [splitMetricIdentity] folds back out; sample start timestamps are not stored by the engine and
// restore as zero.
func appendMetrics(dst *sigmetric.Metrics, c *Chunk) (int, error) {
	id, err := splitMetricIdentity(c.Series)
	if err != nil {
		return 0, err
	}
	if len(c.Values) != len(c.Timestamps) {
		return 0, errors.Errorf("metric series has %d timestamps but %d values", len(c.Timestamps), len(c.Values))
	}

	rm := dst.AddResource()
	rm.Resource = id.Series.Resource.Clone()
	sm := rm.AddScope()
	sm.Scope = id.Series.Scope.Clone()

	mt := sm.AddMetric()
	mt.Name = bytes.Clone(id.Name)
	mt.Unit = bytes.Clone(id.Unit)
	mt.Kind = id.Kind
	mt.Temporality = id.Temporality
	mt.Monotonic = id.Monotonic

	attrs := id.Series.Attributes.Clone()
	for i := range c.Timestamps {
		p := mt.AddPoint()
		p.Attributes = attrs
		p.Ts = c.Timestamps[i]
		p.Value = c.Values[i]
	}
	return len(c.Timestamps), nil
}

// splitMetricIdentity is the inverse of [sigmetric.Identity.ToSeries]: it lifts the reserved
// __name__/__unit__/__kind__/__temporality__/__monotonic__ labels back out of the stored series
// attributes, leaving the data-point attributes behind.
func splitMetricIdentity(s signal.Series) (sigmetric.Identity, error) {
	id := sigmetric.Identity{Series: signal.Series{Resource: s.Resource, Scope: s.Scope}}

	points := make(signal.Attributes, 0, len(s.Attributes))
	var seenName bool
	for _, kv := range s.Attributes {
		switch {
		case bytes.Equal(kv.Key, sigmetric.LabelName):
			id.Name = kv.Value.Str()
			seenName = true
		case bytes.Equal(kv.Key, sigmetric.LabelUnit):
			id.Unit = kv.Value.Str()
		case bytes.Equal(kv.Key, sigmetric.LabelKind):
			id.Kind = sigmetric.PointKind(kv.Value.Int())
		case bytes.Equal(kv.Key, sigmetric.LabelTemporality):
			id.Temporality = sigmetric.Temporality(kv.Value.Int())
		case bytes.Equal(kv.Key, sigmetric.LabelMonotonic):
			id.Monotonic = kv.Value.Bool()
		default:
			points = append(points, kv)
		}
	}
	if !seenName {
		return id, errors.Errorf("metric series carries no %q label", sigmetric.LabelName)
	}

	id.Series.Attributes = points
	return id, nil
}

// addStream appends a (resource, scope) log stream to rl and returns it.
func addStream(rl *siglog.ResourceLogs, s signal.Series) *siglog.ScopeLogs {
	rl.Resource = s.Resource.Clone()
	sl := rl.AddScope()
	sl.Scope = s.Scope.Clone()
	return sl
}

// addSpanStream appends a (resource, scope) span stream to rs and returns it.
func addSpanStream(rs *sigtrace.ResourceSpans, s signal.Series) *sigtrace.ScopeSpans {
	rs.Resource = s.Resource.Clone()
	ss := rs.AddScope()
	ss.Scope = s.Scope.Clone()
	return ss
}

// recordCols indexes a chunk's columns by name so a per-row read is a map lookup rather than a
// linear scan of the column list.
type recordCols map[string]*fetch.NamedColumn

func recordColumns(c *Chunk) recordCols {
	cols := make(recordCols, len(c.Columns))
	for i := range c.Columns {
		cols[c.Columns[i].Name] = &c.Columns[i]
	}
	return cols
}

// int reads row i of an integer column, defaulting to zero when the column is absent or short (a
// backup taken by an older build simply has fewer columns).
func (c recordCols) int(name string, i int) int64 {
	col, ok := c[name]
	if !ok || i >= len(col.Int64) {
		return 0
	}
	return col.Int64[i]
}

// raw reads row i of a bytes column without copying.
func (c recordCols) raw(name string, i int) []byte {
	col, ok := c[name]
	if !ok || i >= len(col.Bytes) {
		return nil
	}
	return col.Bytes[i]
}

// bytes reads row i of a bytes column into owned memory.
func (c recordCols) bytes(name string, i int) []byte {
	return bytes.Clone(c.raw(name, i))
}

// attrs decodes row i of a serialized attributes column. A row whose blob does not decode is
// restored without attributes rather than failing the whole file, matching how the query path
// treats an undecodable attribute blob.
func (c recordCols) attrs(name string, i int) signal.Attributes {
	raw := c.raw(name, i)
	if len(raw) == 0 {
		return nil
	}
	attrs, _, err := signal.DecodeAttributes(raw)
	if err != nil {
		return nil
	}
	return attrs.Clone()
}
