package traceql

import "github.com/go-faster/errors"

// MetricsExpr is a TraceQL metrics query expression.
//
// See https://grafana.com/docs/tempo/latest/traceql/metrics-queries/.
type MetricsExpr interface {
	Expr
	metricsExpr()
}

func (*MetricsAggregation) expr()        {}
func (*MetricsAggregation) metricsExpr() {}
func (*CompareOperation) expr()          {}
func (*CompareOperation) metricsExpr()   {}
func (*MetricsPipeline) expr()           {}
func (*MetricsPipeline) metricsExpr()    {}
func (*MetricsBinaryExpr) expr()         {}
func (*MetricsBinaryExpr) metricsExpr()  {}

// MetricsAggregation aggregates a spanset pipeline into a time series.
type MetricsAggregation struct {
	Op MetricsOp
	// Spanset is a pipeline producing spans to aggregate.
	Spanset Expr
	// Field is an attribute to aggregate, set if [MetricsOp.TakesField].
	Field *Attribute
	// Parameters are quantiles of [MetricsOpQuantileOverTime].
	Parameters []float64
	// By is a set of attributes to group series by, may be empty.
	By []Attribute
}

func (e *MetricsAggregation) validate() error {
	if e.Op.TakesField() {
		if e.Field == nil {
			return errors.Errorf("operation %q requires an attribute argument", e.Op)
		}
	} else if e.Field != nil {
		return errors.Errorf("operation %q takes no arguments", e.Op)
	}

	if e.Op == MetricsOpQuantileOverTime {
		if len(e.Parameters) == 0 {
			return errors.Errorf("operation %q requires at least one quantile", e.Op)
		}
		for _, q := range e.Parameters {
			if q < 0 || q > 1 {
				return errors.Errorf("quantile must be within [0, 1] range, got %v", q)
			}
		}
	} else if len(e.Parameters) > 0 {
		return errors.Errorf("operation %q takes no parameters", e.Op)
	}
	return nil
}

const (
	// DefaultCompareTopN is a default [CompareOperation.TopN].
	DefaultCompareTopN = 10
	// MaxCompareTopN is a maximum [CompareOperation.TopN].
	MaxCompareTopN = 1000
)

// CompareOperation is a `compare()` metrics aggregation.
//
// It splits spans into a selection matching Filter and a baseline of the rest,
// returning a series per attribute value found on them.
type CompareOperation struct {
	// Spanset is a pipeline producing spans to compare.
	Spanset Expr
	// Filter selects spans of the selection group.
	Filter *SpansetFilter
	// TopN limits values returned per attribute.
	TopN int
	// Start and End constrain the selection window, in Unix nanoseconds.
	//
	// Both are zero if unset.
	Start, End int64
}

func (e *CompareOperation) validate() error {
	if e.TopN <= 0 || e.TopN > MaxCompareTopN {
		return errors.Errorf("compare() top number of values must be between 1 and %d, got %d", MaxCompareTopN, e.TopN)
	}
	switch {
	case e.Start == 0 && e.End == 0:
	case e.Start <= 0 || e.End <= 0:
		return errors.New("compare() start and end timestamps must be both set")
	case e.End <= e.Start:
		return errors.New("compare() end timestamp must be greater than start timestamp")
	}
	return nil
}

// MetricsPipeline applies second stage operations to a metrics expression.
type MetricsPipeline struct {
	Expr   MetricsExpr
	Stages []MetricsStage
}

// MetricsBinaryExpr is an arithmetic operation between two metrics expressions.
//
// Op is always arithmetic and never [OpMod] or [OpPow].
type MetricsBinaryExpr struct {
	Left  MetricsExpr
	Op    BinaryOp
	Right MetricsExpr
}

// MetricsStage is a metrics query second stage operation.
//
// See https://grafana.com/docs/tempo/latest/traceql/metrics-queries/#multi-stage-metrics-queries.
type MetricsStage interface {
	metricsStage()
}

func (*TopKOperation) metricsStage()   {}
func (*MetricsFilter) metricsStage()   {}
func (*MetricsScalarOp) metricsStage() {}

// TopKOperation is a `topk()`/`bottomk()` operation, keeping only Limit series
// with the highest (or lowest) values.
type TopKOperation struct {
	Op    MetricsStageOp
	Limit int
}

// MetricsFilter drops series points not matching the comparison.
type MetricsFilter struct {
	Op    BinaryOp
	Value *Static
}

// MetricsScalarOp applies constant arithmetic to every series point.
//
// A duration cannot be used as a scalar operand, so a float carries the value
// without loss.
type MetricsScalarOp struct {
	Op    BinaryOp
	Value float64
	// ScalarLeft whether the constant is the left operand, as in `2 / ({} | rate())`.
	ScalarLeft bool
}
