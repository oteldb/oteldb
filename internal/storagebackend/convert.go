package storagebackend

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// otelAttrs projects storage [signal.Attributes] back into an [otelstorage.Attrs] (a
// pcommon.Map), the representation oteldb's query layer consumes. It is the inverse of the
// pdataconv ingest converter.
func otelAttrs(attrs signal.Attributes) otelstorage.Attrs {
	m := pcommon.NewMap()
	for _, kv := range attrs {
		putSignalValue(m.PutEmpty(string(kv.Key)), kv.Value)
	}
	return otelstorage.Attrs(m)
}

// labelValueString renders a storage value to the exact string the LogQL label set exposes for it.
//
// The log label set is built via [otelAttrs] (signal → pdata) and read back with pcommon's
// AsString (see logqlabels.LabelSet.GetString). A pushdown matcher MUST compare values the same
// way, or it can disagree with the in-memory matchSelector and drop a stream/record the engine
// would keep — a silent false negative. signal.Value.AppendText is not equivalent: it renders bytes
// raw (vs base64) and composite values differently. Projecting through putSignalValue + AsString
// guarantees byte-for-byte parity.
func labelValueString(v signal.Value) string {
	pv := pcommon.NewValueEmpty()
	putSignalValue(pv, v)
	return pv.AsString()
}

// putSignalValue writes a storage [signal.Value] into the pdata value dst, preserving type and
// recursing into slices/maps.
func putSignalValue(dst pcommon.Value, v signal.Value) {
	switch v.Kind() {
	case signal.KindStr:
		dst.SetStr(string(v.Str()))
	case signal.KindBool:
		dst.SetBool(v.Bool())
	case signal.KindInt:
		dst.SetInt(v.Int())
	case signal.KindDouble:
		dst.SetDouble(v.Double())
	case signal.KindBytes:
		dst.SetEmptyBytes().FromRaw(v.Bytes())
	case signal.KindSlice:
		s := dst.SetEmptySlice()
		for _, e := range v.Slice() {
			putSignalValue(s.AppendEmpty(), e)
		}
	case signal.KindMap:
		m := dst.SetEmptyMap()
		for _, kv := range v.Map() {
			putSignalValue(m.PutEmpty(string(kv.Key)), kv.Value)
		}
	default: // KindEmpty
	}
}

// otelTraceID converts a raw 16-byte trace id into an [otelstorage.TraceID]; a short or empty id
// yields the zero (empty) id.
func otelTraceID(b []byte) otelstorage.TraceID {
	var id otelstorage.TraceID
	if len(b) == len(id) {
		copy(id[:], b)
	}
	return id
}

// otelSpanID converts a raw 8-byte span id into an [otelstorage.SpanID]; a short or empty id
// yields the zero (empty) id.
func otelSpanID(b []byte) otelstorage.SpanID {
	var id otelstorage.SpanID
	if len(b) == len(id) {
		copy(id[:], b)
	}
	return id
}
