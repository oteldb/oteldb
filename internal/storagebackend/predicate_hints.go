package storagebackend

import (
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
		c.AnyEqual = fetch.AnyEqualStrings(xregexp.FiniteSet(value.AsString()))
	}
}
