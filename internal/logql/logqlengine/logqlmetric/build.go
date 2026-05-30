package logqlmetric

import (
	"fmt"
	"io"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlerrors"
)

// SampleSelector creates new sampled entry iterator.
type SampleSelector = func(expr *logql.RangeAggregationExpr, start, end time.Time) (iterators.Iterator[SampledEntry], error)

// EvalParams is a query evaluation params.
type EvalParams struct {
	Start, End time.Time
	Step       time.Duration
}

// Build builds new step iterator.
func Build(expr logql.MetricExpr, sel SampleSelector, params EvalParams) (StepIterator, error) {
	return build(expr, sel, params)
}

func build(expr logql.Expr, sel SampleSelector, params EvalParams) (_ StepIterator, rerr error) {
	closeOnError := func(c io.Closer) {
		if rerr != nil {
			_ = c.Close()
		}
	}

	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.RangeAggregationExpr:
		var (
			qrange = expr.Range
			start  = params.Start
			end    = params.End
		)
		if o := qrange.Offset; o != nil {
			start = start.Add(-o.Duration)
			end = end.Add(-o.Duration)
		}
		// Query samples for first step.
		qstart := start.Add(-qrange.Range)

		iter, err := sel(expr, qstart, end)
		if err != nil {
			return nil, errors.Wrap(err, "get samples iterator")
		}
		defer closeOnError(iter)

		return RangeAggregation(iter, expr, start, end, params.Step)
	case *logql.VectorAggregationExpr:
		iter, err := build(expr.Expr, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(iter)

		return VectorAggregation(iter, expr)
	case *logql.LiteralExpr:
	case *logql.LabelReplaceExpr:
	case *logql.VectorExpr:
		return Vector(expr, params.Start, params.End, params.Step), nil
	case *logql.BinOpExpr:
		if lit, ok := expr.Left.(*logql.LiteralExpr); ok {
			right, err := build(expr.Right, sel, params)
			if err != nil {
				return nil, err
			}
			return LiteralBinOp(right, expr, lit.Value, true)
		}
		if lit, ok := expr.Right.(*logql.LiteralExpr); ok {
			left, err := build(expr.Left, sel, params)
			if err != nil {
				return nil, err
			}
			return LiteralBinOp(left, expr, lit.Value, false)
		}

		// TODO(tdakkota): build likely would make a query to storage, so
		// probably we should do it concurrently.
		left, err := build(expr.Left, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(left)

		right, err := build(expr.Right, sel, params)
		if err != nil {
			return nil, err
		}
		defer closeOnError(right)

		return BinOp(left, right, expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
	return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("expression %T is not supported yet", expr)}
}
