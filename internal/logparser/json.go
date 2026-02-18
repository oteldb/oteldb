package logparser

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// GenericJSONParser can parse generic json into [Line].
type GenericJSONParser struct{}

func init() {
	p := &GenericJSONParser{}
	formatRegistry.Store(p.String(), p)
}

func encodeValue(v pcommon.Value, e *jx.Encoder) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		e.Str(v.Str())
	case pcommon.ValueTypeInt:
		e.Int64(v.Int())
	case pcommon.ValueTypeDouble:
		e.Float64(v.Double())
	case pcommon.ValueTypeBool:
		e.Bool(v.Bool())
	case pcommon.ValueTypeBytes:
		e.Base64(v.Bytes().AsRaw())
	case pcommon.ValueTypeEmpty:
		e.Null()
	case pcommon.ValueTypeMap:
		e.Obj(func(e *jx.Encoder) {
			v.Map().Range(func(k string, v pcommon.Value) bool {
				return !e.Field(k, func(e *jx.Encoder) {
					encodeValue(v, e)
				})
			})
		})
	case pcommon.ValueTypeSlice:
		e.Arr(func(e *jx.Encoder) {
			s := v.Slice()
			for i := 0; i < s.Len(); i++ {
				encodeValue(s.At(i), e)
			}
		})
	default:
		panic(fmt.Sprintf("unknown value type %v", v.Type()))
	}
}

func setJSONValue(v pcommon.Value, d *jx.Decoder) error {
	switch d.Next() {
	case jx.String:

		s, err := d.Str()
		if err != nil {
			return errors.Wrap(err, "string")
		}
		v.SetStr(s)
	case jx.Number:
		n, err := d.Num()
		if err != nil {
			return errors.Wrap(err, "number")
		}
		if n.IsInt() {
			i, err := n.Int64()
			if err != nil {
				return errors.Wrap(err, "int")
			}
			v.SetInt(i)
		} else {
			f, err := n.Float64()
			if err != nil {
				return errors.Wrap(err, "float")
			}
			v.SetDouble(f)
		}
	case jx.Bool:
		b, err := d.Bool()
		if err != nil {
			return errors.Wrap(err, "bool")
		}
		v.SetBool(b)
	case jx.Null:
		// Empty
		return nil
	case jx.Array:
		slice := v.SetEmptySlice()
		return d.Arr(func(d *jx.Decoder) error {
			vs := slice.AppendEmpty()
			return setJSONValue(vs, d)
		})
	case jx.Object:
		m := v.SetEmptyMap()
		return d.Obj(func(d *jx.Decoder, key string) error {
			return addJSONMapKey(m, key, d)
		})
	default:
		panic("unreachable")
	}
	return nil
}

func addJSONMapKey(m pcommon.Map, key string, d *jx.Decoder) error {
	switch d.Next() {
	case jx.String:
		v, err := d.Str()
		if err != nil {
			return errors.Wrap(err, "string")
		}
		m.PutStr(key, v)
	case jx.Number:
		v, err := d.Num()
		if err != nil {
			return errors.Wrap(err, "number")
		}
		if v.IsInt() {
			i, err := v.Int64()
			if err != nil {
				return errors.Wrap(err, "int")
			}
			m.PutInt(key, i)
		} else {
			f, err := v.Float64()
			if err != nil {
				return errors.Wrap(err, "float")
			}
			m.PutDouble(key, f)
		}
	case jx.Bool:
		v, err := d.Bool()
		if err != nil {
			return errors.Wrap(err, "bool")
		}
		m.PutBool(key, v)
	case jx.Null:
		m.PutEmpty(key)
	case jx.Array:
		slice := m.PutEmptySlice(key)
		return d.Arr(func(d *jx.Decoder) error {
			v := slice.AppendEmpty()
			return setJSONValue(v, d)
		})
	case jx.Object:
		m2 := m.PutEmptyMap(key)
		return d.Obj(func(d *jx.Decoder, key string) error {
			return addJSONMapKey(m2, key, d)
		})
	default:
		panic("unreachable")
	}
	return nil
}

// Parse generic json into [Line].
func (GenericJSONParser) Parse(data []byte) (*Line, error) {
	dec := jx.DecodeBytes(data)
	const (
		fieldMessage = "message"
		fieldMsg     = "msg"
	)
	hasMsgFields := map[string]bool{
		fieldMessage: false,
		fieldMsg:     false,
	}
	if err := dec.ObjBytes(func(d *jx.Decoder, key []byte) error {
		switch string(key) {
		case fieldMessage:
			hasMsgFields[fieldMessage] = true
		case fieldMsg:
			hasMsgFields[fieldMsg] = true
		}
		return d.Skip()
	}); err != nil {
		return nil, errors.Wrap(err, "read object")
	}

	// Default to "msg".
	// This is the field that will be used for body.
	msgField := fieldMsg
	if hasMsgFields[fieldMessage] && !hasMsgFields[fieldMsg] {
		// Falling back to "message" if "msg" is not present.
		msgField = fieldMessage
	}

	dec.ResetBytes(data)
	line := &Line{}
	attrs := pcommon.NewMap()
	if err := dec.ObjBytes(func(d *jx.Decoder, k []byte) error {
		switch string(k) {
		case "trace_id", "traceid", "traceID", "traceId":
			if d.Next() != jx.String {
				return addJSONMapKey(attrs, string(k), d)
			}
			v, err := d.Str()
			if err != nil {
				return err
			}
			traceID, err := otelstorage.ParseTraceID(strings.ToLower(v))
			if err != nil {
				// Trying to parse as UUID.
				id, err := uuid.Parse(v)
				if err != nil {
					attrs.PutStr(string(k), v)
					return nil
				}
				traceID = otelstorage.TraceID(id)
			}
			line.TraceID = traceID
		case "span_id", "spanid", "spanID", "spanId":
			if d.Next() != jx.String {
				// TODO: handle integers
				return addJSONMapKey(attrs, string(k), d)
			}
			v, err := d.Str()
			if err != nil {
				return err
			}
			raw, _ := hex.DecodeString(v)
			if len(raw) != 8 {
				attrs.PutStr(string(k), v)
				return nil
			}
			var spanID otelstorage.SpanID
			copy(spanID[:], raw)
			line.SpanID = spanID
		case "level", "lvl", "levelStr", "severity_text", "severity", "levelname":
			if d.Next() != jx.String {
				return addJSONMapKey(attrs, string(k), d)
			}
			v, err := d.Str()
			if err != nil {
				return errors.Wrap(err, "level")
			}
			if v == "" {
				attrs.PutStr(string(k), v)
				return nil
			}
			line.SeverityText = v
			line.SeverityNumber = DeduceSeverity(v)
		case msgField:
			if d.Next() != jx.String {
				return addJSONMapKey(attrs, string(k), d)
			}
			v, err := d.Str()
			if err != nil {
				return errors.Wrap(err, "msg")
			}
			line.Body = v
		case "ts", "time", "@timestamp", "timestamp":
			if d.Next() == jx.String {
				v, err := d.Str()
				if err != nil {
					return errors.Wrap(err, "ts")
				}
				if num, err := jx.DecodeStr(v).Num(); err == nil && num.IsInt() {
					// Quoted integer.
					ts, err := num.Int64()
					if err != nil {
						return errors.Wrap(err, "int time")
					}
					if n, ok := DeduceNanos(ts); ok {
						line.Timestamp = otelstorage.Timestamp(n)
						return nil
					}
				}
				for _, layout := range []string{
					time.RFC3339Nano,
					time.RFC3339,
					ISO8601Millis,
				} {
					ts, err := time.Parse(layout, v)
					if err != nil {
						continue
					}
					line.Timestamp = otelstorage.Timestamp(ts.UnixNano())
					return nil
				}
				attrs.PutStr(string(k), v)
				return nil
			} else if d.Next() != jx.Number {
				// Fallback to generic value.
				return addJSONMapKey(attrs, string(k), d)
			}
			v, err := d.Num()
			if err != nil {
				return errors.Wrap(err, "ts")
			}
			if v.IsInt() {
				// Parsing time as integer.
				ts, err := v.Int64()
				if err != nil {
					return errors.Wrap(err, "ts")
				}
				if n, ok := DeduceNanos(ts); ok {
					line.Timestamp = otelstorage.Timestamp(n)
					return nil
				}

				// Fallback.
				attrs.PutInt(string(k), ts)
				return nil
			}

			// Parsing 1318229038.000654 as time.
			// Default is "epoch time, i.e. unix seconds float64".
			// See zapcore.EpochTimeEncoder.
			f, err := v.Float64()
			if err != nil {
				return errors.Wrap(err, "ts parse")
			}
			// TODO: Also deduce f.
			line.Timestamp = otelstorage.Timestamp(f * float64(time.Second))
		default:
			return addJSONMapKey(attrs, string(k), d)
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read object")
	}
	if attrs.Len() > 0 {
		line.Attrs = otelstorage.Attrs(attrs)
	}
	return line, nil
}

// Detect if line is parsable by this parser.
func (GenericJSONParser) Detect(line string) bool {
	return jx.DecodeStr(line).Next() == jx.Object
}

func (GenericJSONParser) String() string {
	return "generic-json"
}
