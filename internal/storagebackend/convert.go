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
