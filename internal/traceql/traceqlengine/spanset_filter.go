package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/traceql"
)

func buildSpansetExpr(expr traceql.SpansetExpr) (Processor, error) {
	switch expr := expr.(type) {
	case *traceql.SpansetFilter:
		return buildSpansetFilter(expr)
	case *traceql.BinarySpansetExpr:
		return buildBinarySpansetExpr(expr)
	default:
		return nil, errors.Errorf("unexpected spanset expression %T", expr)
	}
}

// SpansetFilter filters spansets by field expression.
type SpansetFilter struct {
	Eval Evaluater
}

func buildSpansetFilter(filter *traceql.SpansetFilter) (Processor, error) {
	eval, err := buildEvaluater(filter.Expr)
	if err != nil {
		return nil, errors.Wrap(err, "build spanset evaluater")
	}
	return &SpansetFilter{Eval: eval}, nil
}

// Process implements Processor.
func (f *SpansetFilter) Process(sets []Spanset) (result []Spanset, _ error) {
	return filterBy(f.Eval, sets), nil
}

// BinarySpansetExpr merges two spansets.
type BinarySpansetExpr struct {
	Left  Processor
	Op    SpansetOp
	Right Processor
}

func buildBinarySpansetExpr(expr *traceql.BinarySpansetExpr) (Processor, error) {
	op, err := buildSpansetOp(expr.Op)
	if err != nil {
		return nil, errors.Wrap(err, "build spanset op")
	}

	left, err := buildSpansetExpr(expr.Left)
	if err != nil {
		return nil, errors.Wrap(err, "build left")
	}

	right, err := buildSpansetExpr(expr.Right)
	if err != nil {
		return nil, errors.Wrap(err, "build right")
	}

	return &BinarySpansetExpr{
		Left:  left,
		Op:    op,
		Right: right,
	}, nil
}

// Process implements Processor.
func (f *BinarySpansetExpr) Process(sets []Spanset) (output []Spanset, _ error) {
	return mergeSpansetsBy(sets, f.Left, f.Op, f.Right)
}
