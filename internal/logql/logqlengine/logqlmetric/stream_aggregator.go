package logqlmetric

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
)

// Aggregator is a stateful streaming aggregator.
type Aggregator interface {
	Reset()
	Apply(v float64)
	Result() float64
}

func buildAggregator(expr *logql.VectorAggregationExpr) (func() Aggregator, error) {
	switch expr.Op {
	case logql.VectorOpSum:
		return func() Aggregator {
			return &SumAggregator{}
		}, nil
	case logql.VectorOpAvg:
		return func() Aggregator {
			return &AvgAggregator{}
		}, nil
	case logql.VectorOpCount:
		return func() Aggregator {
			return &CountAggregator{}
		}, nil
	case logql.VectorOpMax:
		return func() Aggregator {
			return &MaxAggregator{}
		}, nil
	case logql.VectorOpMin:
		return func() Aggregator {
			return &MinAggregator{}
		}, nil
	case logql.VectorOpStddev:
		return func() Aggregator {
			return &StddevAggregator{}
		}, nil
	case logql.VectorOpStdvar:
		return func() Aggregator {
			return &StdvarAggregator{}
		}, nil
	default:
		// Sorting and top/bottom-k operations should be handled by caller.
		return nil, errors.Errorf("unexpected vector operation %q", expr.Op)
	}
}

type batchApplier[State any, Agg interface {
	*State
	Aggregator
}] struct{}

// Aggregate implements BatchAggregator.
func (b batchApplier[State, Agg]) Aggregate(points []FPoint) float64 {
	var (
		state State
		agg   Agg = &state
	)
	agg.Reset()
	for _, p := range points {
		agg.Apply(p.Value)
	}
	return agg.Result()
}

// SumAggregator implements moving summing aggregation.
type SumAggregator struct {
	sum float64
}

// Reset implements Aggregator.
func (a *SumAggregator) Reset() {
	a.sum = 0
}

// Apply implements Aggregator.
func (a *SumAggregator) Apply(v float64) {
	a.sum += v
}

// Result implements Aggregator.
func (a *SumAggregator) Result() float64 {
	return a.sum
}

// AvgAggregator implements moving cumulative average aggregation.
type AvgAggregator struct {
	avg, count float64
}

// Reset implements Aggregator.
func (a *AvgAggregator) Reset() {
	a.avg = 0
	a.count = 0
}

// Apply implements Aggregator.
func (a *AvgAggregator) Apply(v float64) {
	if math.IsInf(a.avg, 0) {
		// If ca is Inf, do not try to overwrite it
		if math.IsInf(v, 0) {
			if (a.avg > 0) == (v > 0) {
				return
			}
			// unless new point is Inf with a different sign.
		} else {
			if !math.IsNaN(v) {
				return
			}
			// unless new point is NaN
		}
	}
	// Cumulative average, see https://en.wikipedia.org/wiki/Moving_average#Cumulative_average.
	//
	// 	CA(n+1) = CA(n) + (x(n+1) - CA(n))/(n+1)
	//
	a.count++
	a.avg += (v - a.avg) / a.count
}

// Result implements Aggregator.
func (a *AvgAggregator) Result() float64 {
	return a.avg
}

// CountAggregator implements counting aggregation.
type CountAggregator struct {
	count int64
}

// Reset implements Aggregator.
func (a *CountAggregator) Reset() {
	a.count = 0
}

// Apply implements Aggregator.
func (a *CountAggregator) Apply(float64) {
	a.count++
}

// Result implements Aggregator.
func (a *CountAggregator) Result() float64 {
	return float64(a.count)
}

// MaxAggregator implements max aggregation.
type MaxAggregator struct {
	max float64
	set bool
}

// Reset implements Aggregator.
func (a *MaxAggregator) Reset() {
	a.max = 0
	a.set = false
}

// Apply implements Aggregator.
func (a *MaxAggregator) Apply(v float64) {
	if !a.set || v > a.max || math.IsNaN(v) {
		a.max = v
		a.set = true
	}
}

// Result implements Aggregator.
func (a *MaxAggregator) Result() float64 {
	return a.max
}

// MinAggregator implements min aggregation.
type MinAggregator struct {
	min float64
	set bool
}

// Reset implements Aggregator.
func (a *MinAggregator) Reset() {
	a.min = 0
	a.set = false
}

// Apply implements Aggregator.
func (a *MinAggregator) Apply(v float64) {
	if !a.set || v < a.min || math.IsNaN(v) {
		a.min = v
		a.set = true
	}
}

// Result implements Aggregator.
func (a *MinAggregator) Result() float64 {
	return a.min
}

// StdvarAggregator implements standard variance aggregation.
type StdvarAggregator struct {
	m2    float64
	count float64
	mean  float64
}

// Reset implements Aggregator.
func (a *StdvarAggregator) Reset() {
	a.m2 = 0
	a.count = 0
	a.mean = 0
}

// Apply implements Aggregator.
func (a *StdvarAggregator) Apply(v float64) {
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
	a.count++
	delta := v - a.mean
	a.mean += delta / a.count
	delta2 := v - a.mean
	a.m2 += delta * delta2
}

// Result implements Aggregator.
func (a *StdvarAggregator) Result() float64 {
	return a.m2 / a.count
}

// StddevAggregator implements standard deviation aggregation.
type StddevAggregator struct {
	variance StdvarAggregator
}

// Reset implements Aggregator.
func (a *StddevAggregator) Reset() {
	a.variance.Reset()
}

// Apply implements Aggregator.
func (a *StddevAggregator) Apply(v float64) {
	a.variance.Apply(v)
}

// Result implements Aggregator.
func (a *StddevAggregator) Result() float64 {
	return math.Sqrt(a.variance.Result())
}
