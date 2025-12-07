package chstorage

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
)

// colSimpleAggregateFunction implements a wrapper.
type colSimpleAggregateFunction[T any] struct {
	Function string
	Data     proto.ColumnOf[T]
}

var (
	_ proto.Column       = (*colSimpleAggregateFunction[proto.DateTime64])(nil)
	_ proto.Inferable    = (*colSimpleAggregateFunction[proto.DateTime64])(nil)
	_ proto.StateEncoder = (*colSimpleAggregateFunction[proto.DateTime64])(nil)
	_ proto.StateDecoder = (*colSimpleAggregateFunction[proto.DateTime64])(nil)
	_ proto.Preparable   = (*colSimpleAggregateFunction[proto.DateTime64])(nil)
)

// DecodeColumn implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) DecodeColumn(r *proto.Reader, rows int) error {
	return a.Data.DecodeColumn(r, rows)
}

// EncodeColumn implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) EncodeColumn(b *proto.Buffer) {
	a.Data.EncodeColumn(b)
}

// DecodeState implements [proto.StateDecoder].
func (a *colSimpleAggregateFunction[T]) DecodeState(r *proto.Reader) error {
	if d, ok := a.Data.(proto.StateDecoder); ok {
		return d.DecodeState(r)
	}
	return nil
}

// EncodeState implements [proto.StateEncoder].
func (a *colSimpleAggregateFunction[T]) EncodeState(b *proto.Buffer) {
	if e, ok := a.Data.(proto.StateEncoder); ok {
		e.EncodeState(b)
	}
}

// Prepare implements [proto.Preparable].
func (a *colSimpleAggregateFunction[T]) Prepare() error {
	if p, ok := a.Data.(proto.Preparable); ok {
		return p.Prepare()
	}
	return nil
}

// Reset implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) Reset() {
	a.Data.Reset()
}

// Rows implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) Rows() int {
	return a.Data.Rows()
}

// Type implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) Type() proto.ColumnType {
	return proto.ColumnType(fmt.Sprintf("SimpleAggregateFunction(%s, %s)", a.Function, a.Data.Type()))
}

// WriteColumn implements [proto.Column].
func (a *colSimpleAggregateFunction[T]) WriteColumn(w *proto.Writer) {
	a.Data.WriteColumn(w)
}

// Infer implements [proto.Inferable].
func (a *colSimpleAggregateFunction[T]) Infer(t proto.ColumnType) error {
	raw := strings.TrimSpace(string(t))

	raw, ok := strings.CutPrefix(raw, "SimpleAggregateFunction(")
	if !ok {
		return errors.Errorf("unexpected type %q", t)
	}
	_, typ, ok := strings.Cut(raw, ",")
	if !ok {
		return errors.Errorf("expected function and type, got: %q", raw)
	}
	t = proto.ColumnType(strings.TrimSuffix(typ, ")"))

	if i, ok := a.Data.(proto.Inferable); ok {
		return i.Infer(t)
	}
	return nil
}

// Append wraps [proto.ColumnOf.Append].
func (a *colSimpleAggregateFunction[T]) Append(val T) {
	a.Data.Append(val)
}

// Row wraps [proto.ColumnOf.Row].
func (a *colSimpleAggregateFunction[T]) Row(i int) T {
	return a.Data.Row(i)
}
