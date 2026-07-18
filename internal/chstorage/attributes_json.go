package chstorage

import (
	"slices"
	"strings"

	"github.com/go-faster/jx"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

type lazyAttributes struct {
	orig pcommon.Map
}

func (l *lazyAttributes) Attributes(additional ...[2]string) otelstorage.Attrs {
	if len(additional) > 0 {
		target := pcommon.NewMap()
		l.orig.CopyTo(target)
		for _, pair := range additional {
			target.PutStr(pair[0], pair[1])
		}
		return otelstorage.Attrs(target)
	}
	return otelstorage.Attrs(l.orig)
}

func encodeAttributes(attrs pcommon.Map, additional ...[2]string) []byte {
	e := &jx.Encoder{}
	encodeMap(e, attrs, additional...)
	return e.Bytes()
}

func encodeValue(e *jx.Encoder, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		e.Str(v.Str())
	case pcommon.ValueTypeInt:
		e.Int64(v.Int())
	case pcommon.ValueTypeDouble:
		e.Float64(v.Double())
	case pcommon.ValueTypeBool:
		e.Bool(v.Bool())
	case pcommon.ValueTypeMap:
		m := v.Map()
		encodeMap(e, m)
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		e.ArrStart()
		for i := 0; i < s.Len(); i++ {
			encodeValue(e, s.At(i))
		}
		e.ArrEnd()
	case pcommon.ValueTypeBytes:
		e.ByteStr(v.Bytes().AsRaw())
	default:
		e.Null()
	}
}

func encodeMap(e *jx.Encoder, m pcommon.Map, additional ...[2]string) {
	if otelstorage.Attrs(m).IsZero() && len(additional) == 0 {
		e.ObjEmpty()
		return
	}

	type value struct {
		str  string
		otel pcommon.Value
	}
	type pair struct {
		key   string
		value value
	}
	var pairs []pair

	const stackThreshold = otelstorage.MapStackThreshold
	if l := m.Len(); l < stackThreshold {
		pairs = make([]pair, 0, stackThreshold)
	} else {
		pairs = make([]pair, 0, m.Len()+len(additional))
	}

	m.Range(func(k string, v pcommon.Value) bool {
		pairs = append(pairs, pair{
			key:   k,
			value: value{otel: v},
		})
		return true
	})
	for _, p := range additional {
		pairs = append(pairs, pair{
			key:   p[0],
			value: value{str: p[1]},
		})
	}

	slices.SortFunc(pairs, func(a, b pair) int {
		// [cmp.Compare] has significantly worse performance
		// than [strings.Compare] due to NaN checks.
		//
		// Note that [strings.Compare] is not intrinsified, although
		// SIMD-based implementation would not give any performance
		// gains, since attribute names tend to be relatively small (<64 bytes).
		//
		// See https://go.dev/issue/61725.
		return strings.Compare(a.key, b.key)
	})

	e.ObjStart()
	for _, p := range pairs {
		e.FieldStart(p.key)
		if v := p.value.otel; v != (pcommon.Value{}) {
			encodeValue(e, v)
		} else {
			e.Str(p.value.str)
		}
	}
	e.ObjEnd()
}

func decodeAttributes(s []byte) (otelstorage.Attrs, error) {
	result := pcommon.NewMap()
	if len(s) == 0 {
		return otelstorage.Attrs(result), nil
	}
	err := decodeMap(jx.DecodeBytes(s), result)
	return otelstorage.Attrs(result), err
}

func decodeValue(d *jx.Decoder, val pcommon.Value) error {
	switch d.Next() {
	case jx.String:
		v, err := d.Str()
		if err != nil {
			return err
		}
		val.SetStr(v)
		return nil
	case jx.Number:
		n, err := d.Num()
		if err != nil {
			return err
		}
		if n.IsInt() {
			if v, err := n.Int64(); err == nil {
				val.SetInt(v)
				return nil
			}
			// An integer outside the int64 range (e.g. a uint64 id/hash) does not fit pcommon's int
			// value. Preserve it exactly as a string rather than failing the whole block decode.
			val.SetStr(n.String())
			return nil
		}
		v, err := n.Float64()
		if err != nil {
			return err
		}
		val.SetDouble(v)
		return nil
	case jx.Null:
		if err := d.Null(); err != nil {
			return err
		}
		// Do nothing, keep value empty.
		return nil
	case jx.Bool:
		v, err := d.Bool()
		if err != nil {
			return err
		}
		val.SetBool(v)
		return nil
	case jx.Array:
		s := val.SetEmptySlice()
		return d.Arr(func(d *jx.Decoder) error {
			rval := s.AppendEmpty()
			return decodeValue(d, rval)
		})
	case jx.Object:
		m := val.SetEmptyMap()
		return decodeMap(d, m)
	default:
		return d.Skip()
	}
}

func decodeMap(d *jx.Decoder, m pcommon.Map) error {
	return d.Obj(func(d *jx.Decoder, key string) error {
		rval := m.PutEmpty(key)
		return decodeValue(d, rval)
	})
}

func hashTimeseries(name string, res, scope, attrs otelstorage.Attrs) [16]byte {
	h := xxh3.New()

	_, _ = h.WriteString(name)
	otelstorage.WriteAttrHash(h, res.AsMap())
	otelstorage.WriteAttrHash(h, scope.AsMap())
	otelstorage.WriteAttrHash(h, attrs.AsMap())

	hash := h.Sum128().Bytes()
	return hash
}
