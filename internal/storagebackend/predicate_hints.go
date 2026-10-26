package storagebackend

import (
	"bytes"
	"iter"
	"slices"

	"github.com/oteldb/storage/query/fetch"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/xregexp"
)

func rawStringConditionHints(c *fetch.Condition, op traceql.BinaryOp, value traceql.Static) {
	if value.Type != traceql.TypeString {
		return
	}
	switch op {
	case traceql.OpEq:
		c.Equal = &fetch.EqualMatcher{Name: c.Column, Value: value.AsString()}
	case traceql.OpRe:
		c.AnyEqual = finiteRegexpValues(value.AsString())
	}
}

func finiteRegexpValues(pattern string) [][]byte {
	return stringValues(xregexp.FiniteSet(pattern))
}

func stringValues(seq iter.Seq[string]) [][]byte {
	var values [][]byte
	for value := range seq {
		values = append(values, []byte(value))
	}
	slices.SortFunc(values, bytes.Compare)
	return slices.CompactFunc(values, bytes.Equal)
}
