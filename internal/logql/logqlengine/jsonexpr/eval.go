package jsonexpr

import (
	"strconv"

	"github.com/go-faster/jx"

	"github.com/oteldb/oteldb/internal/logql"
)

// Extract extracts values from given paths using extract callback.
func Extract(
	d *jx.Decoder,
	paths SelectorTree,
	extract func(logql.Label, string),
) error {
	e := extractor{
		paths:   paths,
		extract: extract,
	}
	return e.walk(e.paths.root, d)
}

// extractor extracts values from JSON value.
type extractor struct {
	paths   SelectorTree
	extract func(logql.Label, string)
}

func (e *extractor) walk(n *selectorNode, d *jx.Decoder) error {
	switch d.Next() {
	case jx.String:
		val, err := d.Str()
		if err != nil {
			return err
		}
		e.matchLiteral(n, val)
		return nil
	case jx.Number:
		num, err := d.Num()
		if err != nil {
			return err
		}
		e.matchLiteral(n, string(num))
		return nil
	case jx.Null:
		if err := d.Null(); err != nil {
			return err
		}
		e.matchLiteral(n, "")
		return nil
	case jx.Bool:
		val, err := d.Bool()
		if err != nil {
			return err
		}
		e.matchLiteral(n, strconv.FormatBool(val))
		return nil
	case jx.Array:
		return e.walkArr(n, d)
	case jx.Object:
		return e.walkObj(n, d)
	default:
		return d.Skip()
	}
}

func (e *extractor) walkObj(n *selectorNode, d *jx.Decoder) error {
	if err := e.tryMatchRaw(n, d); err != nil {
		return err
	}

	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		child, ok := n.key[string(key)]
		if !ok {
			return d.Skip()
		}

		if err := e.walk(child, d); err != nil {
			return err
		}
		return nil
	})
}

func (e *extractor) walkArr(n *selectorNode, d *jx.Decoder) error {
	if err := e.tryMatchRaw(n, d); err != nil {
		return err
	}

	c := 0
	return d.Arr(func(d *jx.Decoder) error {
		child, ok := n.index[c]
		if !ok {
			return d.Skip()
		}

		if err := e.walk(child, d); err != nil {
			return err
		}
		c++
		return nil
	})
}

func (e *extractor) matchLiteral(n *selectorNode, val string) {
	for _, label := range n.labels {
		e.extract(label, val)
	}
}

func (e *extractor) tryMatchRaw(n *selectorNode, d *jx.Decoder) error {
	if len(n.labels) == 0 {
		return nil
	}

	var (
		raw string
		got bool
	)
	if err := d.Capture(func(d *jx.Decoder) error {
		data, err := d.Raw()
		if err != nil {
			return err
		}
		raw = string(data)
		got = true
		return nil
	}); err != nil {
		return err
	}
	if got {
		for _, label := range n.labels {
			e.extract(label, raw)
		}
	}
	return nil
}
