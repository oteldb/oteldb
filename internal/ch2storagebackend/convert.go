package ch2storagebackend

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// attrConv projects pcommon-backed attribute maps into the engine's [signal.Attributes],
// memoizing by the map's content hash.
//
// chstorage hands out the *same* resource, scope, and series attribute maps for every row of a
// series, so without memoization the projection cost scales with rows rather than with distinct
// attribute sets. The cache is scoped to one migration and bounded by the source's distinct
// resource/scope/series cardinality.
type attrConv struct {
	cache map[otelstorage.Hash]signal.Attributes
}

func newAttrConv() *attrConv {
	return &attrConv{cache: map[otelstorage.Hash]signal.Attributes{}}
}

// convert projects a, reusing a previously converted result for an identical attribute set. Use it
// for resource, scope, and series attributes — the sets shared across many rows. Per-row attributes
// go through [convertAttrs], since caching a set seen once costs more than it saves.
func (c *attrConv) convert(a otelstorage.Attrs) signal.Attributes {
	if a.Len() == 0 {
		return nil
	}

	key := a.Hash()
	if attrs, ok := c.cache[key]; ok {
		return attrs
	}

	attrs := convertMap(a.AsMap())
	c.cache[key] = attrs
	return attrs
}

// convertAttrs projects an attribute set, tolerating the zero [otelstorage.Attrs] (a nil-backed
// map) that a row carries when the column held no attributes.
func convertAttrs(a otelstorage.Attrs) signal.Attributes {
	if a.Len() == 0 {
		return nil
	}
	return convertMap(a.AsMap())
}

// convertMap projects an OTLP attribute map to internal typed [signal.Attributes]. Byte values are
// copied out of the pdata buffers so the internal model owns them.
func convertMap(m pcommon.Map) signal.Attributes {
	if m.Len() == 0 {
		return nil
	}

	kvs := make([]signal.KeyValue, 0, m.Len())
	for k, v := range m.All() {
		kvs = append(kvs, signal.KeyValue{Key: []byte(k), Value: convertValue(v)})
	}
	return signal.NewAttributes(kvs...)
}

// convertValue projects an OTLP AnyValue to the internal typed [signal.Value], preserving type
// (and recursing into slices/maps).
func convertValue(v pcommon.Value) signal.Value {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return signal.StringValue([]byte(v.Str()))
	case pcommon.ValueTypeBool:
		return signal.BoolValue(v.Bool())
	case pcommon.ValueTypeInt:
		return signal.IntValue(v.Int())
	case pcommon.ValueTypeDouble:
		return signal.DoubleValue(v.Double())
	case pcommon.ValueTypeBytes:
		return signal.BytesValue(v.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		vs := make([]signal.Value, s.Len())
		for i := range s.Len() {
			vs[i] = convertValue(s.At(i))
		}
		return signal.SliceValue(vs...)
	case pcommon.ValueTypeMap:
		return signal.MapValue(convertMap(v.Map())...)
	default: // ValueTypeEmpty
		return signal.EmptyValue()
	}
}

// traceIDBytes copies a 16-byte trace id into an owned slice, returning nil when unset.
func traceIDBytes(id otelstorage.TraceID) []byte {
	if id.IsEmpty() {
		return nil
	}
	return append([]byte(nil), id[:]...)
}

// spanIDBytes copies an 8-byte span id into an owned slice, returning nil when unset.
func spanIDBytes(id otelstorage.SpanID) []byte {
	if id.IsEmpty() {
		return nil
	}
	return append([]byte(nil), id[:]...)
}

// streamKey groups rows into one (resource, scope) stream. Resource and scope attribute sets are
// identified by content hash; logs and traces additionally carry a scope name/version, which
// chstorage stores separately from the scope attribute map.
type streamKey struct {
	resource     otelstorage.Hash
	scope        otelstorage.Hash
	scopeName    string
	scopeVersion string
}

// streamPos locates a stream's scope slot inside a batch by index rather than by pointer.
//
// The engine's Add* builders append into retained backing arrays, so appending a later resource or
// scope can reallocate an earlier one's slice — a pointer cached across the batch would then be
// writing into an abandoned array. Indices survive the reallocation; pointers do not.
type streamPos struct {
	resource int
	scope    int
}

// ConvertLogs projects chstorage log records into the engine's log batch, grouping them into one
// stream per (resource, scope) — the same grouping [logstorage.RecordsToLogs] applies when building
// the equivalent OTLP payload.
func ConvertLogs(records []logstorage.Record, c *attrConv) siglog.Logs {
	var (
		dst     siglog.Logs
		streams = map[streamKey]streamPos{}
	)

	for _, r := range records {
		key := streamKey{
			resource:     r.ResourceAttrs.Hash(),
			scope:        r.ScopeAttrs.Hash(),
			scopeName:    r.ScopeName,
			scopeVersion: r.ScopeVersion,
		}

		pos, ok := streams[key]
		if !ok {
			rl := dst.AddResource()
			rl.Resource = signal.Resource{Attributes: c.convert(r.ResourceAttrs)}

			sl := rl.AddScope()
			sl.Scope = signal.Scope{
				Name:       []byte(r.ScopeName),
				Version:    []byte(r.ScopeVersion),
				Attributes: c.convert(r.ScopeAttrs),
			}

			pos = streamPos{resource: len(dst.Resources) - 1, scope: len(rl.Scopes) - 1}
			streams[key] = pos
		}

		rec := dst.Resources[pos.resource].Scopes[pos.scope].AddRecord()
		rec.Timestamp = int64(r.Timestamp)
		rec.ObservedTimestamp = int64(r.ObservedTimestamp)
		rec.SeverityNumber = int32(r.SeverityNumber)
		rec.SeverityText = []byte(r.SeverityText)
		rec.Body = []byte(r.Body)
		rec.TraceID = traceIDBytes(r.TraceID)
		rec.SpanID = spanIDBytes(r.SpanID)
		rec.Flags = uint32(r.Flags)
		rec.Dropped = 0
		rec.Attributes = convertAttrs(r.Attrs)
	}

	return dst
}

// ConvertTraces projects chstorage spans into the engine's trace batch, grouping them into one
// stream per (resource, scope) — the same grouping [tracestorage.SpansToTraces] applies when
// building the equivalent OTLP payload.
func ConvertTraces(spans []tracestorage.Span, c *attrConv) sigtrace.Traces {
	var (
		dst     sigtrace.Traces
		streams = map[streamKey]streamPos{}
	)

	for _, s := range spans {
		key := streamKey{
			resource:     s.ResourceAttrs.Hash(),
			scope:        s.ScopeAttrs.Hash(),
			scopeName:    s.ScopeName,
			scopeVersion: s.ScopeVersion,
		}

		pos, ok := streams[key]
		if !ok {
			rs := dst.AddResource()
			rs.Resource = signal.Resource{Attributes: c.convert(s.ResourceAttrs)}

			ss := rs.AddScope()
			ss.Scope = signal.Scope{
				Name:       []byte(s.ScopeName),
				Version:    []byte(s.ScopeVersion),
				Attributes: c.convert(s.ScopeAttrs),
			}

			pos = streamPos{resource: len(dst.Resources) - 1, scope: len(rs.Scopes) - 1}
			streams[key] = pos
		}

		sp := dst.Resources[pos.resource].Scopes[pos.scope].AddSpan()
		sp.Attributes = convertAttrs(s.Attrs)
		sp.TraceID = traceIDBytes(s.TraceID)
		sp.SpanID = spanIDBytes(s.SpanID)
		sp.ParentSpanID = spanIDBytes(s.ParentSpanID)
		sp.Name = []byte(s.Name)
		sp.StatusMessage = []byte(s.StatusMessage)
		sp.TraceState = []byte(s.TraceState)
		sp.Start = int64(s.Start)
		sp.End = int64(s.End)
		sp.Kind = s.Kind
		sp.StatusCode = s.StatusCode
		sp.Flags = 0
		sp.Dropped = 0

		for _, ev := range s.Events {
			e := sp.AddEvent()
			e.Time = int64(ev.Timestamp)
			e.Name = []byte(ev.Name)
			e.Attributes = convertAttrs(ev.Attrs)
			e.Dropped = 0
		}

		for _, ln := range s.Links {
			l := sp.AddLink()
			l.TraceID = traceIDBytes(ln.TraceID)
			l.SpanID = spanIDBytes(ln.SpanID)
			l.TraceState = []byte(ln.TraceState)
			l.Attributes = convertAttrs(ln.Attrs)
			l.Dropped = 0
		}
	}

	return dst
}

// metricKey names one Metric within a stream. chstorage stores decomposed Prometheus-style series,
// so points of the same name accumulate under a single gauge.
type metricKey struct {
	stream streamKey
	name   string
}

// ConvertNumberPoints projects chstorage's decomposed numeric samples into the engine's metric
// batch, one gauge per (resource, scope, name) — matching [metricstorage.NumberPointsToMetrics].
// chstorage discards the original gauge-vs-sum distinction for these points, so every series is
// emitted as a gauge; values, names, and labels round-trip exactly.
//
// Only the number-point path is direct. Exponential histograms keep the pdata route, because the
// engine decomposes them into classic _count/_sum/_bucket{le} series inside its pdataconv bridge
// and that bucket math is not reachable from here; re-deriving it would duplicate subtle logic for
// a small minority of points.
func ConvertNumberPoints(points []metricstorage.NumberPoint, c *attrConv) sigmetric.Metrics {
	var (
		dst     sigmetric.Metrics
		streams = map[streamKey]streamPos{}
		metrics = map[metricKey]int{}
	)

	for _, p := range points {
		// chstorage does not store a scope name/version for metrics, only a scope attribute map,
		// so the stream is identified by the two attribute hashes alone.
		skey := streamKey{resource: p.Resource.Hash(), scope: p.Scope.Hash()}

		spos, ok := streams[skey]
		if !ok {
			rm := dst.AddResource()
			rm.Resource = signal.Resource{Attributes: c.convert(p.Resource)}

			sm := rm.AddScope()
			sm.Scope = signal.Scope{Attributes: c.convert(p.Scope)}

			spos = streamPos{resource: len(dst.Resources) - 1, scope: len(rm.Scopes) - 1}
			streams[skey] = spos
		}
		sm := &dst.Resources[spos.resource].Scopes[spos.scope]

		mkey := metricKey{stream: skey, name: p.Name}
		mi, ok := metrics[mkey]
		if !ok {
			mt := sm.AddMetric()
			mt.Name = []byte(p.Name)
			mt.Unit = []byte(p.Unit)
			mt.Kind = sigmetric.KindGauge

			mi = len(sm.Metrics) - 1
			metrics[mkey] = mi
		}

		pt := sm.Metrics[mi].AddPoint()
		pt.Attributes = c.convert(p.Attrs)
		pt.Ts = int64(p.Timestamp)
		pt.Value = p.Value
	}

	return dst
}
