package logqlmetric

import (
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
)

// BatchAggregator is stateless batch aggregator.
type BatchAggregator interface {
	Aggregate(points []FPoint) float64
}

func buildBatchAggregator(expr *logql.RangeAggregationExpr) (BatchAggregator, error) {
	qrange := expr.Range
	switch expr.Op {
	case logql.RangeOpCount:
		return &CountOverTime{}, nil
	case logql.RangeOpRate:
		if qrange.Unwrap == nil {
			return &Rate[CountOverTime]{selRange: qrange.Range.Seconds()}, nil
		}
		return &Rate[SumOverTime]{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpRateCounter:
		return &RateCounter{selRange: qrange.Range}, nil
	case logql.RangeOpBytes:
		return &SumOverTime{}, nil
	case logql.RangeOpBytesRate:
		return &BytesRate{selRange: qrange.Range.Seconds()}, nil
	case logql.RangeOpAvg:
		return &AvgOverTime{}, nil
	case logql.RangeOpSum:
		return &SumOverTime{}, nil
	case logql.RangeOpMin:
		return &MinOverTime{}, nil
	case logql.RangeOpMax:
		return &MaxOverTime{}, nil
	case logql.RangeOpStdvar:
		return &StdvarOverTime{}, nil
	case logql.RangeOpStddev:
		return &StddevOverTime{}, nil
	case logql.RangeOpQuantile:
		p := expr.Parameter
		if p == nil {
			return nil, errors.Errorf("operation %q require a parameter", expr.Op)
		}
		return &QuantileOverTime{param: *p}, nil
	case logql.RangeOpFirst:
		return &FirstOverTime{}, nil
	case logql.RangeOpLast:
		return &LastOverTime{}, nil
	case logql.RangeOpAbsent:
		return &absentOverTime{}, nil
	default:
		return nil, errors.Errorf("unexpected range operation %q", expr.Op)
	}
}

// CountOverTime implements `count_over_time` aggregation.
type CountOverTime struct{}

// Aggregate implements BatchAggregator.
func (CountOverTime) Aggregate(points []FPoint) float64 {
	return float64(len(points))
}

// Rate implements `rate` aggregation.
type Rate[A BatchAggregator] struct {
	preAgg   A
	selRange float64
}

// Aggregate implements BatchAggregator.
func (a Rate[A]) Aggregate(points []FPoint) float64 {
	return a.preAgg.Aggregate(points) / a.selRange
}

// RateCounter implements `rate_counter` aggregation.
type RateCounter struct {
	selRange time.Duration
}

// Aggregate implements BatchAggregator.
func (a RateCounter) Aggregate(points []FPoint) float64 {
	return extrapolatedRate(points, a.selRange, true, true)
}

// BytesRate implements `bytes_rate` aggregation.
type BytesRate = Rate[SumOverTime]

// AvgOverTime implements `avg_over_time` aggregation.
type AvgOverTime = batchApplier[AvgAggregator, *AvgAggregator]

// SumOverTime implements `sum_over_time` aggregation.
type SumOverTime = batchApplier[SumAggregator, *SumAggregator]

// MinOverTime implements `min_over_time` aggregation.
type MinOverTime = batchApplier[MinAggregator, *MinAggregator]

// MaxOverTime implements `max_over_time` aggregation.
type MaxOverTime = batchApplier[MaxAggregator, *MaxAggregator]

// StdvarOverTime implements `stdvar_over_time` aggregation.
type StdvarOverTime = batchApplier[StdvarAggregator, *StdvarAggregator]

// StddevOverTime implements `stddev_over_time` aggregation.
type StddevOverTime = batchApplier[StddevAggregator, *StddevAggregator]

// QuantileOverTime implements `quantile_over_time` aggregation.
type QuantileOverTime struct {
	param float64
}

// Aggregate implements BatchAggregator.
func (a QuantileOverTime) Aggregate(points []FPoint) float64 {
	return quantile(a.param, points)
}

// FirstOverTime implements `first_over_time` aggregation.
type FirstOverTime struct{}

// Aggregate implements BatchAggregator.
func (FirstOverTime) Aggregate(points []FPoint) float64 {
	if len(points) == 0 {
		return 0
	}
	return points[0].Value
}

// LastOverTime implements `last_over_time` aggregation.
type LastOverTime struct{}

// Aggregate implements BatchAggregator.
func (LastOverTime) Aggregate(points []FPoint) (last float64) {
	if len(points) == 0 {
		return 0
	}
	return points[len(points)-1].Value
}

// absentOverTime implements `absent_over_time` aggregation.
type absentOverTime struct{}

// Aggregate implements BatchAggregator.
func (absentOverTime) Aggregate([]FPoint) float64 {
	return 1.
}
