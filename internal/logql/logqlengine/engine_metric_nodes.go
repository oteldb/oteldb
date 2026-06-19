package logqlengine

import (
	"context"
	"io"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlmetric"
)

var (
	_ MetricNode = (*RangeAggregation)(nil)
	_ MetricNode = (*VectorAggregation)(nil)
	_ MetricNode = (*LabelReplace)(nil)
	_ MetricNode = (*Vector)(nil)
	_ MetricNode = (*LiteralBinOp)(nil)
	_ MetricNode = (*BinOp)(nil)
)

// RangeAggregation is a [MetricNode] implementing range aggregation.
type RangeAggregation struct {
	Input SampleNode
	Expr  *logql.RangeAggregationExpr
}

// Traverse implements [Node].
func (n *RangeAggregation) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalMetric implements [EvalMetric].
func (n *RangeAggregation) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	var (
		qrange = n.Expr.Range
		start  = params.Start
		end    = params.End
	)
	if o := qrange.Offset; o != nil {
		start = start.Add(-o.Duration)
		end = end.Add(-o.Duration)
	}

	// If the input can bucket samples per step itself (offloaded to storage,
	// see chstorage.SamplingNode), skip streaming raw samples here and let it
	// aggregate in SQL instead. The optimizer only offloads to a
	// BucketedSampleNode when qrange.Offset/Unwrap are nil, so no extra
	// eligibility check is needed here.
	if bn, ok := n.Input.(BucketedSampleNode); ok {
		iter, err := bn.EvalBucketedSample(ctx, EvalParams{
			Start:     start,
			End:       end,
			Step:      params.Step,
			Direction: DirectionForward,
			Limit:     -1,
		}, qrange.Range)
		if err != nil {
			return nil, err
		}
		defer closeOnError(iter, &rerr)

		switch n.Expr.Op {
		case logql.RangeOpRate, logql.RangeOpBytesRate:
			return logqlmetric.DivideStep(iter, qrange.Range.Seconds()), nil
		default:
			return iter, nil
		}
	}

	// Query samples for first step.
	qstart := start.Add(-qrange.Range)

	iter, err := n.Input.EvalSample(ctx, EvalParams{
		Start:     qstart,
		End:       end,
		Step:      params.Step,
		Direction: DirectionForward,
		// Do not limit sample queries.
		Limit: -1,
	})
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return logqlmetric.RangeAggregation(iter, n.Expr, start, end, params.Step)
}

// VectorAggregation is a [MetricNode] implementing vector aggregation.
type VectorAggregation struct {
	Input MetricNode
	Expr  *logql.VectorAggregationExpr
}

// Traverse implements [Node].
func (n *VectorAggregation) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalMetric implements [EvalMetric].
func (n *VectorAggregation) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	iter, err := n.Input.EvalMetric(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return logqlmetric.VectorAggregation(iter, n.Expr)
}

// LabelReplace is a [MetricNode] implementing `label_replace` function.
type LabelReplace struct {
	Input MetricNode
	Expr  *logql.LabelReplaceExpr
}

// Traverse implements [Node].
func (n *LabelReplace) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalMetric implements [EvalMetric].
func (n *LabelReplace) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	iter, err := n.Input.EvalMetric(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return logqlmetric.LabelReplace(iter, n.Expr)
}

// Vector is a [MetricNode] implementing vector literal.
type Vector struct {
	Expr *logql.VectorExpr
}

// Traverse implements [Node].
func (n *Vector) Traverse(cb NodeVisitor) error {
	return cb(n)
}

// EvalMetric implements [EvalMetric].
func (n *Vector) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	return logqlmetric.Vector(n.Expr, params.Start, params.End, params.Step), nil
}

// LiteralBinOp is a [MetricNode] implementing binary operation on literal.
type LiteralBinOp struct {
	Input           MetricNode
	Literal         float64
	IsLiteralOnLeft bool
	Expr            *logql.BinOpExpr
}

// Traverse implements [Node].
func (n *LiteralBinOp) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalMetric implements [EvalMetric].
func (n *LiteralBinOp) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	iter, err := n.Input.EvalMetric(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return logqlmetric.LiteralBinOp(iter, n.Expr, n.Literal, n.IsLiteralOnLeft)
}

// BinOp is a [MetricNode] implementing binary operation.
type BinOp struct {
	Left, Right MetricNode
	Expr        *logql.BinOpExpr
}

// Traverse implements [Node].
func (n *BinOp) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	if err := n.Left.Traverse(cb); err != nil {
		return err
	}
	return n.Right.Traverse(cb)
}

// EvalMetric implements [EvalMetric].
func (n *BinOp) EvalMetric(ctx context.Context, params MetricParams) (_ StepIterator, rerr error) {
	// TODO(tdakkota): it is likely it would make a query to storage, so
	// probably we should do it concurrently.
	left, err := n.Left.EvalMetric(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(left, &rerr)

	right, err := n.Right.EvalMetric(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(right, &rerr)

	return logqlmetric.BinOp(left, right, n.Expr)
}

func closeOnError[C io.Closer](c C, rerr *error) {
	if *rerr != nil {
		_ = c.Close()
	}
}
