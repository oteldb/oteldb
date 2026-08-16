// Package otlpdirect decodes OTLP protobuf straight into the storage engine's ingest models.
//
// It replaces the receiver → pdata → pdataconv path with a single pass over the request bytes:
// attribute keys, string values, log bodies and ids are handed to the engine as sub-slices of the
// request buffer, never copied into Go strings and never materialized as pdata. The engine copies
// what it retains (record cells into its column blobs, identity into its symbol table), so the
// request buffer may be recycled once the write returns.
//
// The wire layout is OTLP's, so the field numbers here are the ones in opentelemetry-proto and
// must not be renumbered. Unknown fields are skipped, which is what makes a newer collector's
// output readable by an older ingester.
package otlpdirect

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/xarena"
)

// Field numbers of the messages shared by every signal (common.proto, resource.proto).
const (
	// opentelemetry.proto.common.v1.AnyValue
	fieldAnyString = 1
	fieldAnyBool   = 2
	fieldAnyInt    = 3
	fieldAnyDouble = 4
	fieldAnyArray  = 5
	fieldAnyKvlist = 6
	fieldAnyBytes  = 7

	// opentelemetry.proto.common.v1.KeyValue
	fieldKVKey   = 1
	fieldKVValue = 2

	// opentelemetry.proto.common.v1.ArrayValue / KeyValueList
	fieldListValues = 1

	// opentelemetry.proto.common.v1.InstrumentationScope
	fieldScopeName       = 1
	fieldScopeVersion    = 2
	fieldScopeAttributes = 3

	// opentelemetry.proto.resource.v1.Resource
	fieldResourceAttributes = 1
)

// decoder holds the scratch the converters carve identity out of. Chunks are retained across
// [decoder.reset], so a converter reused across requests allocates nothing in steady state.
type decoder struct {
	attrs   xarena.Arena[signal.KeyValue]
	values  xarena.Arena[signal.Value]
	scratch xarena.Arena[byte]
}

func (d *decoder) reset() {
	d.attrs.Reset()
	d.values.Reset()
	d.scratch.Reset()
}

// attributes decodes a repeated KeyValue field into a sorted [signal.Attributes].
//
// src is the concatenation of the message's KeyValue submessages, collected by the caller as it
// walks the parent — protobuf permits a repeated field's entries to be interleaved with others, so
// they cannot be counted up front without a second pass.
func (d *decoder) attributes(kvs [][]byte) (signal.Attributes, error) {
	if len(kvs) == 0 {
		return nil, nil
	}

	out := d.attrs.Alloc(len(kvs))

	for _, src := range kvs {
		kv, err := d.keyValue(src)
		if err != nil {
			return nil, err
		}

		out = append(out, kv)
	}

	// NewAttributes sorts by key, which is what makes the identity hash stable regardless of the
	// order a producer emitted them in.
	return signal.NewAttributes(out...), nil
}

func (d *decoder) keyValue(src []byte) (signal.KeyValue, error) {
	var (
		fc  easyproto.FieldContext
		kv  signal.KeyValue
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return kv, errors.Wrap(err, "read key-value field")
		}

		switch fc.FieldNum {
		case fieldKVKey:
			key, ok := fc.Bytes()
			if !ok {
				return kv, errors.New("read attribute key")
			}

			kv.Key = key
		case fieldKVValue:
			data, ok := fc.MessageData()
			if !ok {
				return kv, errors.New("read attribute value")
			}

			if kv.Value, err = d.anyValue(data); err != nil {
				return kv, err
			}
		}
	}

	return kv, nil
}

// anyValue decodes an AnyValue, preserving its type. A later field wins, matching how a protobuf
// oneof decodes.
func (d *decoder) anyValue(src []byte) (signal.Value, error) {
	var (
		fc  easyproto.FieldContext
		out = signal.EmptyValue()
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return out, errors.Wrap(err, "read value field")
		}

		switch fc.FieldNum {
		case fieldAnyString:
			v, ok := fc.Bytes()
			if !ok {
				return out, errors.New("read string value")
			}

			out = signal.StringValue(v)
		case fieldAnyBool:
			v, ok := fc.Bool()
			if !ok {
				return out, errors.New("read bool value")
			}

			out = signal.BoolValue(v)
		case fieldAnyInt:
			v, ok := fc.Int64()
			if !ok {
				return out, errors.New("read int value")
			}

			out = signal.IntValue(v)
		case fieldAnyDouble:
			v, ok := fc.Double()
			if !ok {
				return out, errors.New("read double value")
			}

			out = signal.DoubleValue(v)
		case fieldAnyBytes:
			v, ok := fc.Bytes()
			if !ok {
				return out, errors.New("read bytes value")
			}

			out = signal.BytesValue(v)
		case fieldAnyArray:
			data, ok := fc.MessageData()
			if !ok {
				return out, errors.New("read array value")
			}

			if out, err = d.arrayValue(data); err != nil {
				return out, err
			}
		case fieldAnyKvlist:
			data, ok := fc.MessageData()
			if !ok {
				return out, errors.New("read kvlist value")
			}

			if out, err = d.kvlistValue(data); err != nil {
				return out, err
			}
		}
	}

	return out, nil
}

func (d *decoder) arrayValue(src []byte) (signal.Value, error) {
	entries, err := collect(src, fieldListValues, "array entry")
	if err != nil {
		return signal.EmptyValue(), err
	}

	vals := d.values.Alloc(len(entries))

	for _, e := range entries {
		v, err := d.anyValue(e)
		if err != nil {
			return signal.EmptyValue(), err
		}

		vals = append(vals, v)
	}

	return signal.SliceValue(vals...), nil
}

func (d *decoder) kvlistValue(src []byte) (signal.Value, error) {
	entries, err := collect(src, fieldListValues, "kvlist entry")
	if err != nil {
		return signal.EmptyValue(), err
	}

	attrs, err := d.attributes(entries)
	if err != nil {
		return signal.EmptyValue(), err
	}

	return signal.MapValue(attrs...), nil
}

// resource decodes a Resource message's attributes. schemaURL is carried by the parent, so it is
// set by the caller.
func (d *decoder) resource(src []byte) (signal.Attributes, error) {
	kvs, err := collect(src, fieldResourceAttributes, "resource attribute")
	if err != nil {
		return nil, err
	}

	return d.attributes(kvs)
}

func (d *decoder) scope(src []byte) (signal.Scope, error) {
	var (
		fc  easyproto.FieldContext
		sc  signal.Scope
		kvs [][]byte
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return sc, errors.Wrap(err, "read scope field")
		}

		switch fc.FieldNum {
		case fieldScopeName:
			v, ok := fc.Bytes()
			if !ok {
				return sc, errors.New("read scope name")
			}

			sc.Name = v
		case fieldScopeVersion:
			v, ok := fc.Bytes()
			if !ok {
				return sc, errors.New("read scope version")
			}

			sc.Version = v
		case fieldScopeAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return sc, errors.New("read scope attribute")
			}

			kvs = append(kvs, data)
		}
	}

	if sc.Attributes, err = d.attributes(kvs); err != nil {
		return sc, err
	}

	return sc, nil
}

// collect gathers every occurrence of a repeated submessage field.
//
// Every caller happens to pass 1 today, because OTLP puts the repeated member first in the
// messages that hold nothing else. That is a convention, not a guarantee, and naming the field at
// each call site is what makes those walks readable.
//
//nolint:unparam // see above
func collect(src []byte, fieldNum uint32, what string) ([][]byte, error) {
	var (
		fc  easyproto.FieldContext
		out [][]byte
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return nil, errors.Wrapf(err, "read %s", what)
		}

		if fc.FieldNum != fieldNum {
			continue
		}

		data, ok := fc.MessageData()
		if !ok {
			return nil, errors.Errorf("read %s data", what)
		}

		out = append(out, data)
	}

	return out, nil
}

// renderText renders a value to its OTLP textual form, matching pcommon.Value.AsString — the form the
// pdata path stores a non-string log body as. A string value is returned as-is (aliasing the
// request buffer); every other kind is rendered into the arena.
func (d *decoder) renderText(v signal.Value) []byte {
	switch v.Kind() {
	case signal.KindStr:
		return v.Str()
	case signal.KindEmpty:
		return nil
	case signal.KindBool:
		return strconv.AppendBool(d.scratch.Alloc(8), v.Bool())
	case signal.KindInt:
		return strconv.AppendInt(d.scratch.Alloc(24), v.Int(), 10)
	case signal.KindDouble:
		return appendFloat(d.scratch.Alloc(32), v.Double())
	case signal.KindBytes:
		raw := v.Bytes()
		out := d.scratch.Alloc(base64.StdEncoding.EncodedLen(len(raw)))[:base64.StdEncoding.EncodedLen(len(raw))]
		base64.StdEncoding.Encode(out, raw)

		return out
	default: // slice and map render as JSON, as pdata does
		return marshalJSON(v)
	}
}

// appendFloat renders a float the way pdata does: %v-equivalent shortest form, but always with a
// decimal point or exponent so an integral value is still recognizable as a float.
func appendFloat(dst []byte, f float64) []byte {
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

// marshalJSON renders a slice or map value as JSON with HTML escaping off, matching pdata's
// marshalJSONNoHTMLEscape. It allocates: a structured log body is the uncommon case, and an
// encoder's growth cannot be hosted in the arena.
func marshalJSON(v signal.Value) []byte {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(rawValue(v)); err != nil {
		return nil
	}

	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")) // Encode appends one; AsString has none
}

// rawValue projects a value to the plain Go tree pdata's AsRaw produces, so the JSON rendering of
// a structured body matches byte for byte.
func rawValue(v signal.Value) any {
	switch v.Kind() {
	case signal.KindStr:
		return string(v.Str())
	case signal.KindBool:
		return v.Bool()
	case signal.KindInt:
		return v.Int()
	case signal.KindDouble:
		return v.Double()
	case signal.KindBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case signal.KindSlice:
		src := v.Slice()

		out := make([]any, len(src))
		for i := range src {
			out[i] = rawValue(src[i])
		}

		return out
	case signal.KindMap:
		out := map[string]any{}
		for _, kv := range v.Map() {
			out[string(kv.Key)] = rawValue(kv.Value)
		}

		return out
	default:
		return nil
	}
}
