package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func buildAggregator(expr *traceql.AggregateScalarExpr) (_ Evaluater, err error) {
	var by Evaluater
	if expr.Op != traceql.AggregateOpCount {
		by, err = buildEvaluater(expr.Field)
		if err != nil {
			return nil, errors.Wrap(err, "build aggregate evaluater")
		}
	}

	switch expr.Op {
	case traceql.AggregateOpCount:
		return &AggregateEvalauter[CountAgg]{CountAgg{}}, nil
	case traceql.AggregateOpMax:
		return &AggregateEvalauter[MaxAgg]{MaxAgg{By: by}}, nil
	case traceql.AggregateOpMin:
		return &AggregateEvalauter[MinAgg]{MinAgg{By: by}}, nil
	case traceql.AggregateOpAvg:
		return &AggregateEvalauter[AvgAgg]{AvgAgg{Sum: SumAgg{By: by}}}, nil
	case traceql.AggregateOpSum:
		return &AggregateEvalauter[SumAgg]{SumAgg{By: by}}, nil
	default:
		return nil, errors.Errorf("unexpected aggregate op %q", expr.Op)
	}
}

// AggregateEvalauter evaluates aggregation expression.
type AggregateEvalauter[A Aggregator] struct {
	Agg A
}

// Eval implemenets [Evaluater].
func (e *AggregateEvalauter[A]) Eval(_ tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	r.SetNumber(e.Agg.Aggregate(ctx.Set))
	return r
}

// Aggregator is an aggregation expression.
type Aggregator interface {
	Aggregate(set Spanset) float64
}

// CountAgg is a `count()` aggregator.
type CountAgg struct{}

// Aggregate implements [Aggregator].
func (CountAgg) Aggregate(set Spanset) float64 {
	return float64(len(set.Spans))
}

// MaxAgg is `max(...)` aggregator.
type MaxAgg struct {
	By Evaluater
}

// Aggregate implements [Aggregator].
func (agg MaxAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx      = set.evaluateCtx()
		resultSet bool
	)
	for _, span := range set.Spans {
		result := agg.By.Eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		if val := result.ToFloat(); !resultSet || val > r {
			r = val
			resultSet = true
		}
	}
	return r
}

// MinAgg is `min(...)` aggregator.
type MinAgg struct {
	By Evaluater
}

// Aggregate implements [Aggregator].
func (agg MinAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx      = set.evaluateCtx()
		resultSet bool
	)
	for _, span := range set.Spans {
		result := agg.By.Eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		if val := result.ToFloat(); !resultSet || val < r {
			r = val
			resultSet = true
		}
	}
	return r
}

// AvgAgg is `avg(...)` aggregator.
type AvgAgg struct {
	Sum SumAgg
}

// Aggregate implements [Aggregator].
func (agg AvgAgg) Aggregate(set Spanset) float64 {
	count := len(set.Spans)
	if count == 0 {
		return 0
	}

	sum := agg.Sum.Aggregate(set)
	return sum / float64(count)
}

// SumAgg is `sum(...)` aggregator.
type SumAgg struct {
	By Evaluater
}

// Aggregate implements [Aggregator].
func (agg SumAgg) Aggregate(set Spanset) (r float64) {
	var (
		ectx = set.evaluateCtx()
		sum  float64
	)
	for _, span := range set.Spans {
		result := agg.By.Eval(span, ectx)
		if !result.Type.IsNumeric() {
			continue
		}
		sum += result.ToFloat()
	}
	return sum
}
