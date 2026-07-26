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
