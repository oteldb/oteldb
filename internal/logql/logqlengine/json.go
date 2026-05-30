package logqlengine

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/jsonexpr"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// JSONExtractor is a JSON label extractor.
type JSONExtractor struct {
	paths  jsonexpr.SelectorTree
	labels map[logql.Label]struct{}
}

func buildJSONExtractor(stage *logql.JSONExpressionParser) (Processor, error) {
	var (
		exprs  = stage.Exprs
		labels = stage.Labels
	)

	e := &JSONExtractor{}
	switch {
	case len(exprs) > 0:
		paths := make(map[logql.Label]jsonexpr.Path, len(labels)+len(exprs))
		for _, p := range exprs {
			sel, err := jsonexpr.Parse(p.Expr)
			if err != nil {
				return nil, errors.Wrapf(err, "parse selector %q", p.Expr)
			}
			paths[p.Label] = sel
		}
		// Convert labels into selectors.
		for _, label := range labels {
			paths[label] = jsonexpr.Path{
				jsonexpr.KeySel(string(label)),
			}
		}
		e.paths = jsonexpr.MakeSelectorTree(paths)
		return e, nil
	case len(labels) > 0:
		e.labels = make(map[logql.Label]struct{}, len(labels))
		for _, label := range labels {
			e.labels[label] = struct{}{}
		}
		return e, nil
	default:
		return e, nil
	}
}

// Process implements Processor.
func (e *JSONExtractor) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	var err error
	switch {
	case !e.paths.IsEmpty():
		err = extractExprs(e.paths, line, set)
	case len(e.labels) != 0:
		err = extractSome(e.labels, line, set)
	default:
		err = extractAll(line, set)
	}
	if err != nil {
		set.SetError("JSON parsing error", err)
	}
	return line, true
}

func extractExprs(paths jsonexpr.SelectorTree, line string, set logqlabels.LabelSet) error {
	// TODO(tdakkota): allocates buffer for each line.
	d := decodeStr(line)
	return jsonexpr.Extract(
		d,
		paths,
		func(l logql.Label, s string) {
			set.Set(l, pcommon.NewValueStr(s))
		},
	)
}

func extractSome(labels map[logql.Label]struct{}, line string, set logqlabels.LabelSet) error {
	d := decodeStr(line)
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		if _, ok := labels[logql.Label(key)]; !ok {
			return d.Skip()
		}
		value, ok, err := parseValue(d)
		if err != nil {
			return errors.Wrapf(err, "parse label %q", key)
		}
		if !ok {
			return nil
		}
		// TODO(tdakkota): try string interning
		// TODO(tdakkota): probably, we can just use label name string
		// 	instead of allocating a new string every time
		set.Set(logql.Label(key), value)
		return nil
	})
}

func extractAll(line string, set logqlabels.LabelSet) error {
	d := decodeStr(line)
	return d.Obj(func(d *jx.Decoder, key string) error {
		value, ok, err := parseValue(d)
		if err != nil {
			return errors.Wrapf(err, "parse label %q", key)
		}
		if !ok {
			return nil
		}
		// TODO(tdakkota): try string interning
		set.Set(logql.Label(otelstorage.KeyToLabel(key)), value)
		return nil
	})
}

func parseValue(d *jx.Decoder) (pcommon.Value, bool, error) {
	var val pcommon.Value
	switch tt := d.Next(); tt {
	case jx.String:
		str, err := d.Str()
		if err != nil {
			return val, false, err
		}
		val = pcommon.NewValueStr(str)
	case jx.Number:
		num, err := d.Num()
		if err != nil {
			return val, false, err
		}
		if num.IsInt() {
			n, err := num.Int64()
			if err != nil {
				return val, false, err
			}
			val = pcommon.NewValueInt(n)
		} else {
			n, err := num.Float64()
			if err != nil {
				return val, false, err
			}
			val = pcommon.NewValueDouble(n)
		}
	case jx.Null:
		err := d.Null()
		return val, false, err
	case jx.Bool:
		b, err := d.Bool()
		if err != nil {
			return val, false, err
		}
		val = pcommon.NewValueBool(b)
	case jx.Array:
		val = pcommon.NewValueSlice()
		slice := val.Slice()

		if err := d.Arr(func(d *jx.Decoder) error {
			elem, ok, err := parseValue(d)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			item := slice.AppendEmpty()
			elem.CopyTo(item)
			return nil
		}); err != nil {
			return val, false, err
		}
	case jx.Object:
		val = pcommon.NewValueMap()
		m := val.Map()

		if err := d.Obj(func(d *jx.Decoder, k string) error {
			elem, ok, err := parseValue(d)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			item := m.PutEmpty(k)
			elem.CopyTo(item)
			return nil
		}); err != nil {
			return val, false, err
		}
	default:
		return val, false, errors.Errorf("unexpected type %q", tt)
	}
	return val, true, nil
}
