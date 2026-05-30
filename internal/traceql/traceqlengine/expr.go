package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/traceql"
)

// BuildExpr builds given TraceQL expression.
func BuildExpr(expr traceql.Expr) (Processor, error) {
	switch expr := expr.(type) {
	case *traceql.BinaryExpr:
		return BuildBinaryExpr(expr)
	case *traceql.SpansetPipeline:
		return BuildPipeline(expr.Pipeline...)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
}

// BinaryExpr merges two pipelines.
type BinaryExpr struct {
	left  Processor
	op    SpansetOp
	right Processor
}

// BuildBinaryExpr builds a new binary expression processor.
func BuildBinaryExpr(expr *traceql.BinaryExpr) (Processor, error) {
	op, err := buildSpansetOp(expr.Op)
	if err != nil {
		return nil, errors.Wrap(err, "build spanset op")
	}

	left, err := BuildExpr(expr.Left)
	if err != nil {
		return nil, errors.Wrap(err, "build left")
	}

	right, err := BuildExpr(expr.Right)
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
func (f *BinaryExpr) Process(sets []Spanset) ([]Spanset, error) {
	return mergeSpansetsBy(sets, f.left, f.op, f.right)
}
