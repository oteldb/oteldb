package scarecrow

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// dateFuncs are the argument-optional date/time instant functions: given a vector, they map each
// value (a unix timestamp in seconds) to a component of it; given none, they read the query's own
// step timestamp instead. Both shapes go through [stepDateFn] or [unaryFn] with one of these as
// the value transform — the functions themselves know nothing about time being current or not.
var dateFuncs = map[string]func(time.Time) float64{
	"days_in_month": func(t time.Time) float64 {
		return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
	},
	"day_of_month": func(t time.Time) float64 { return float64(t.Day()) },
	"day_of_week":  func(t time.Time) float64 { return float64(t.Weekday()) },
	"day_of_year":  func(t time.Time) float64 { return float64(t.YearDay()) },
	"hour":         func(t time.Time) float64 { return float64(t.Hour()) },
	"minute":       func(t time.Time) float64 { return float64(t.Minute()) },
	"month":        func(t time.Time) float64 { return float64(t.Month()) },
	"year":         func(t time.Time) float64 { return float64(t.Year()) },
}

// unixSecondsToDateFn adapts a date/time function to [unaryFn]'s per-sample shape: the input
// value is a unix timestamp in seconds, matching upstream's dateWrapper.
func unixSecondsToDateFn(fn func(time.Time) float64) func(float64) float64 {
	return func(v float64) float64 { return fn(time.Unix(int64(v), 0).UTC()) }
}

// stepDateFn emits a single, unlabeled series whose value at each step is a pure function of
// that step's own timestamp: time() and the argument-less form of a date/time function
// (`year()`, as opposed to `year(some_vector)`). Unlike [numberLiteral] its value is not
// constant across steps.
type stepDateFn struct {
	name string
	ec   *EvalContext
	// scalar selects the output schema: time() is a PromQL scalar, but the date/time functions
	// (even called with no argument) return a one-row instant vector — see
	// docs/promql-engine.md's function-tail note and upstream's parser.Functions table.
	scalar bool
	fn     func(stepMs int64) float64

	out  Column
	done bool
}

func newStepTimeFn(ec *EvalContext) *stepDateFn {
	return &stepDateFn{
		name:   "time",
		ec:     ec,
		scalar: true,
		fn:     func(stepMs int64) float64 { return float64(stepMs) / 1000 },
	}
}

func newStepDateFn(name string, dateFn func(time.Time) float64, ec *EvalContext) *stepDateFn {
	return &stepDateFn{
		name: name,
		ec:   ec,
		fn:   func(stepMs int64) float64 { return dateFn(time.Unix(stepMs/1000, 0).UTC()) },
	}
}

func (o *stepDateFn) String() string { return fmt.Sprintf("StepDateFn(%s)", o.name) }

func (o *stepDateFn) Children() []Operator { return nil }

func (o *stepDateFn) Close() error { return nil }

func (o *stepDateFn) Schema(context.Context) (*Schema, error) {
	if o.scalar {
		return ScalarSchema(), nil
	}

	return NewSchema([]labels.Labels{labels.EmptyLabels()}), nil
}

func (o *stepDateFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	o.out.Resize(0, o.ec.NumSteps())
	for i, t := range o.ec.Steps {
		o.out.Set(i, o.fn(t))
	}

	return &o.out, nil
}
