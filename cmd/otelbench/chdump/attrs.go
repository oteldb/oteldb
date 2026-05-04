package chdump

import (
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

type Attributes struct {
	index  *proto.ColBytes
	col    *proto.ColLowCardinalityRaw
	hashes map[otelstorage.Hash]int

	// values are filled up only when decoding.
	values []otelstorage.Attrs
}

// NewAttributes creates a new [Attributes].
func NewAttributes() *Attributes {
	ac := &Attributes{
		index:  new(proto.ColBytes),
		hashes: map[otelstorage.Hash]int{},
	}
	ac.col = &proto.ColLowCardinalityRaw{
		Index: ac.index,
		Key:   proto.KeyUInt64,
	}
	return ac
}

func (a Attributes) Type() proto.ColumnType {
	return proto.ColumnTypeLowCardinality.Sub(proto.ColumnTypeString)
}

func (a Attributes) Rows() int {
	return a.col.Rows()
}

func (a *Attributes) DecodeColumn(r *proto.Reader, rows int) error {
	if err := a.col.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "col")
	}
	for i := 0; i < a.index.Rows(); i++ {
		v := a.index.Row(i)
		m, err := decodeAttributes(v)
		if err != nil {
			return errors.Wrapf(err, "index value %d", i)
		}
		a.hashes[m.Hash()] = i
		a.values = append(a.values, m)
	}
	return nil
}

func (a *Attributes) Reset() {
	a.col.Reset()
	a.index.Reset()
	a.values = a.values[:0]
	clear(a.hashes)
	a.col.Key = proto.KeyUInt64
}

func (a *Attributes) DecodeState(r *proto.Reader) error {
	return a.col.DecodeState(r)
}

func (a *Attributes) rowIdx(i int) int {
	switch a.col.Key {
	case proto.KeyUInt8:
		return int(a.col.Keys8[i])
	case proto.KeyUInt16:
		return int(a.col.Keys16[i])
	case proto.KeyUInt32:
		return int(a.col.Keys32[i])
	case proto.KeyUInt64:
		return int(a.col.Keys64[i])
	default:
		panic("invalid key type")
	}
}

func (a *Attributes) Row(i int) otelstorage.Attrs {
	return a.values[a.rowIdx(i)]
}
