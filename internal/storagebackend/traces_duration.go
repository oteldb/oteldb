package storagebackend

import (
	"github.com/oteldb/storage/query/fetch"

	"github.com/oteldb/oteldb/internal/traceql"
)

func durationRangeHint(op traceql.BinaryOp, value traceql.Static) *fetch.Int64Range {
	if value.Type != traceql.TypeDuration {
		return nil
	}
	bound := &fetch.Int64Bound{Value: int64(value.AsDuration())}
	switch op {
	case traceql.OpEq:
		bound.Inclusive = true
		return &fetch.Int64Range{Lower: bound, Upper: bound}
	case traceql.OpGt, traceql.OpGte:
		bound.Inclusive = op == traceql.OpGte
		return &fetch.Int64Range{Lower: bound}
	case traceql.OpLt, traceql.OpLte:
		bound.Inclusive = op == traceql.OpLte
		return &fetch.Int64Range{Upper: bound}
	default:
		return nil
	}
}
