package traceqlengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/traceql"
)

// MetricsSource plans first-stage span aggregations over ec's bucket grid.
// Implementations own filtering, grouping, sampling, read budgets, and duration
// conversion to seconds. Schemas must be stable and group identities lossless;
// NaN gaps must be absent in columns. Each call returns a fresh operator, charging
// bucket values through [scarecrow.EvalContext.ChargeSamples]. Successful calls
// transfer operator ownership; exemplars and typed output metadata remain with source.
type MetricsSource interface {
	Aggregation(context.Context, *traceql.MetricsAggregation, *scarecrow.EvalContext) (scarecrow.Operator, error)
}

// MetricsCompareSource optionally plans compare's selection/baseline aggregation.
type MetricsCompareSource interface {
	Compare(context.Context, *traceql.CompareOperation, *scarecrow.EvalContext) (scarecrow.Operator, error)
}

// BuildMetrics lowers a metrics expression to shared column operators and resolves
// its schemas. The caller must close a successful result. On error it closes all
// operators acquired from source. It does not persist metrics or implement HTTP APIs.
func BuildMetrics(ctx context.Context, expr traceql.MetricsExpr, source MetricsSource, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || ec == nil || len(ec.Steps) == 0 {
		return nil, errors.New("metrics planning requires a source and nonempty evaluation grid")
	}
	p := metricsPlanner{source: source, ec: ec}
	root, err := p.build(ctx, expr)
	if err != nil {
		return nil, err
	}
	if err := scarecrow.Prepare(ctx, root); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return root, nil
}

type metricsPlanner struct {
	source MetricsSource
	ec     *scarecrow.EvalContext
}

func (p *metricsPlanner) build(ctx context.Context, expr traceql.MetricsExpr) (scarecrow.Operator, error) {
	switch expr := expr.(type) {
	case *traceql.MetricsAggregation:
		root, err := p.source.Aggregation(ctx, expr, p.ec)
		return checkedMetricSource(root, err)
	case *traceql.CompareOperation:
		if source, ok := p.source.(MetricsCompareSource); ok {
			root, err := source.Compare(ctx, expr, p.ec)
			return checkedMetricSource(root, err)
		}
		return nil, &UnsupportedError{Msg: "metrics source does not support compare"}
	case *traceql.MetricsBinaryExpr:
		left, err := p.build(ctx, expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.build(ctx, expr.Right)
		if err != nil {
			return nil, errors.Join(err, left.Close())
		}
		op, err := arithmeticMetricOp(expr.Op)
		if err != nil {
			return nil, errors.Join(err, left.Close(), right.Close())
		}
		root, err := scarecrow.NewVectorBinary(left, right, op, traceMetricsBinaryOptions(), p.ec)
		if err != nil {
			return nil, errors.Join(err, left.Close(), right.Close())
		}
		return root, nil
	case *traceql.MetricsPipeline:
		root, err := p.build(ctx, expr.Expr)
		if err != nil {
			return nil, err
		}
		for _, stage := range expr.Stages {
			next, err := p.stage(root, stage)
			if err != nil {
				return nil, errors.Join(err, root.Close())
			}
			root = next
		}
		return root, nil
	default:
		return nil, &UnsupportedError{Msg: "unsupported metrics expression"}
	}
}

func checkedMetricSource(root scarecrow.Operator, err error) (scarecrow.Operator, error) {
	if err != nil {
		if root != nil {
			err = errors.Join(err, root.Close())
		}
		return nil, err
	}
	if root == nil {
		return nil, errors.New("metrics source returned no operator")
	}
	return root, nil
}

func traceMetricsBinaryOptions() scarecrow.BinaryOptions {
	return scarecrow.BinaryOptions{
		NaNOnZeroDivision:   true,
		NaNIsAbsent:         true,
		PreserveMetricNames: true,
		BroadcastUnlabelled: true,
	}
}

func (p *metricsPlanner) stage(input scarecrow.Operator, stage traceql.MetricsStage) (scarecrow.Operator, error) {
	switch stage := stage.(type) {
	case *traceql.TopKOperation:
		switch stage.Op {
		case traceql.MetricsStageOpTopK:
			return scarecrow.NewTopK(input, stage.Limit, p.ec)
		case traceql.MetricsStageOpBottomK:
			return scarecrow.NewBottomK(input, stage.Limit, p.ec)
		}
	case *traceql.MetricsFilter:
		op, err := comparisonMetricOp(stage.Op)
		if err != nil {
			return nil, err
		}
		if stage.Value == nil || !stage.Value.Type.IsNumeric() {
			return nil, errors.New("metrics filter requires a numeric threshold")
		}
		value := stage.Value.ToFloat()
		if stage.Value.Type == traceql.TypeDuration {
			value = stage.Value.AsDuration().Seconds()
		}
		return scarecrow.NewScalarBinary(input, value, op, false, traceMetricsBinaryOptions(), p.ec)
	case *traceql.MetricsScalarOp:
		op, err := arithmeticMetricOp(stage.Op)
		if err != nil {
			return nil, err
		}
		return scarecrow.NewScalarBinary(input, stage.Value, op, stage.ScalarLeft, traceMetricsBinaryOptions(), p.ec)
	}
	return nil, &UnsupportedError{Msg: "unsupported metrics stage"}
}

func arithmeticMetricOp(op traceql.BinaryOp) (scarecrow.BinaryOp, error) {
	switch op {
	case traceql.OpAdd:
		return scarecrow.BinaryAdd, nil
	case traceql.OpSub:
		return scarecrow.BinarySub, nil
	case traceql.OpMul:
		return scarecrow.BinaryMul, nil
	case traceql.OpDiv:
		return scarecrow.BinaryDiv, nil
	default:
		return 0, &UnsupportedError{Msg: "unsupported metrics arithmetic operation"}
	}
}

func comparisonMetricOp(op traceql.BinaryOp) (scarecrow.BinaryOp, error) {
	switch op {
	case traceql.OpEq:
		return scarecrow.BinaryEqual, nil
	case traceql.OpNotEq:
		return scarecrow.BinaryNotEqual, nil
	case traceql.OpGt:
		return scarecrow.BinaryGreater, nil
	case traceql.OpGte:
		return scarecrow.BinaryGreaterEqual, nil
	case traceql.OpLt:
		return scarecrow.BinaryLess, nil
	case traceql.OpLte:
		return scarecrow.BinaryLessEqual, nil
	default:
		return 0, &UnsupportedError{Msg: "unsupported metrics comparison operation"}
	}
}
