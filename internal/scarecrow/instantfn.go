package scarecrow

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// unaryFuncs are the instant functions that map one sample to one sample. They are the
// streaming shape: one input column in, one output column out, with no accumulator at all.
var unaryFuncs = map[string]func(float64) float64{
	"abs":   math.Abs,
	"ceil":  math.Ceil,
	"floor": math.Floor,
	"exp":   math.Exp,
	"ln":    math.Log,
	"log2":  math.Log2,
	"log10": math.Log10,
	"sqrt":  math.Sqrt,
	"sgn": func(v float64) float64 {
		switch {
		case v < 0:
			return -1
		case v > 0:
			return 1
		default:
			return v // preserves NaN and signed zero
		}
	},
	"acos":  math.Acos,
	"acosh": math.Acosh,
	"asin":  math.Asin,
	"asinh": math.Asinh,
	"atan":  math.Atan,
	"atanh": math.Atanh,
	"cos":   math.Cos,
	"cosh":  math.Cosh,
	"sin":   math.Sin,
	"sinh":  math.Sinh,
	"tan":   math.Tan,
	"tanh":  math.Tanh,
	"deg":   func(v float64) float64 { return v * 180 / math.Pi },
	"rad":   func(v float64) float64 { return v * math.Pi / 180 },
}

// unaryFn applies a per-sample function to its input, dropping __name__ because the result is
// no longer the input metric.
type unaryFn struct {
	input Operator
	name  string
	fn    func(float64) float64

	schema *Schema
	out    Column
}

func newUnaryFn(input Operator, name string, fn func(float64) float64) *unaryFn {
	return &unaryFn{input: input, name: name, fn: fn}
}

func (o *unaryFn) String() string { return fmt.Sprintf("UnaryFn(%s)", o.name) }

func (o *unaryFn) Children() []Operator { return []Operator{o.input} }

func (o *unaryFn) Close() error { return o.input.Close() }

func (o *unaryFn) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	series := make([]labels.Labels, in.Len())
	for i, ls := range in.Series {
		series[i] = dropMetricName(ls)
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *unaryFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)
	for i := range o.out.V {
		if o.out.IsSet(i) {
			o.out.V[i] = o.fn(o.out.V[i])
		}
	}

	return &o.out, nil
}

// negate implements the unary minus operator. It keeps the series set unchanged apart from
// __name__, which unary minus drops as arithmetic does.
type negate struct{ *unaryFn }

func newNegate(input Operator) *negate {
	return &negate{unaryFn: newUnaryFn(input, "-", func(v float64) float64 { return -v })}
}

func (o *negate) String() string { return "Negate" }

// clampFn implements clamp, clamp_min and clamp_max, whose bounds are scalars evaluated per
// step rather than constants.
type clampFn struct {
	input    Operator
	name     string
	minInput Operator
	maxInput Operator
	ec       *EvalContext

	schema   *Schema
	minCol   []float64
	maxCol   []float64
	resolved bool
	out      Column
}

func newClampFn(input Operator, name string, minInput, maxInput Operator, ec *EvalContext) *clampFn {
	return &clampFn{input: input, name: name, minInput: minInput, maxInput: maxInput, ec: ec}
}

func (o *clampFn) String() string { return fmt.Sprintf("Clamp(%s)", o.name) }

func (o *clampFn) Children() []Operator {
	out := []Operator{o.input}
	if o.minInput != nil {
		out = append(out, o.minInput)
	}

	if o.maxInput != nil {
		out = append(out, o.maxInput)
	}

	return out
}

func (o *clampFn) Close() error { return o.input.Close() }

func (o *clampFn) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	series := make([]labels.Labels, in.Len())
	for i, ls := range in.Series {
		series[i] = dropMetricName(ls)
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *clampFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !o.resolved {
		o.resolved = true

		var err error
		if o.minCol, err = scalarValues(ctx, o.minInput, math.Inf(-1), o.ec.NumSteps()); err != nil {
			return nil, err
		}

		if o.maxCol, err = scalarValues(ctx, o.maxInput, math.Inf(1), o.ec.NumSteps()); err != nil {
			return nil, err
		}
	}

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)

	for i := range o.out.V {
		if !o.out.IsSet(i) {
			continue
		}

		lo, hi := o.minCol[i], o.maxCol[i]
		// An inverted range yields no sample at all, matching upstream's clamp.
		if lo > hi {
			o.out.Clear(i)
			continue
		}

		o.out.V[i] = math.Max(lo, math.Min(hi, o.out.V[i]))
	}

	if o.out.Empty() {
		// Signal end-of-stream only when the input is exhausted, not when a column empties, so
		// recurse to the next input series.
		return o.Next(ctx)
	}

	return &o.out, nil
}

// scalarValues drains a scalar operator into a per-step value slice. It always returns exactly
// steps values, filling def where the operator is absent or produced no sample, so callers can
// index it unconditionally.
func scalarValues(ctx context.Context, op Operator, def float64, steps int) ([]float64, error) {
	out := make([]float64, steps)
	for i := range out {
		out[i] = def
	}

	if op == nil {
		return out, nil
	}

	col, err := op.Next(ctx)
	if err != nil {
		return nil, err
	}

	if col == nil {
		return out, nil
	}

	for i := range min(steps, col.Steps()) {
		if col.IsSet(i) {
			out[i] = col.V[i]
		}
	}

	return out, nil
}

// roundFn implements round, whose optional second argument is a per-step scalar.
type roundFn struct {
	input   Operator
	toArg   Operator
	ec      *EvalContext
	schema  *Schema
	toNear  []float64
	loaded  bool
	out     Column
	dropped bool
}

func newRoundFn(input, toArg Operator, ec *EvalContext) *roundFn {
	return &roundFn{input: input, toArg: toArg, ec: ec}
}

func (o *roundFn) String() string { return "Round" }

func (o *roundFn) Children() []Operator {
	if o.toArg == nil {
		return []Operator{o.input}
	}

	return []Operator{o.input, o.toArg}
}

func (o *roundFn) Close() error { return o.input.Close() }

func (o *roundFn) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	series := make([]labels.Labels, in.Len())
	for i, ls := range in.Series {
		series[i] = dropMetricName(ls)
	}

	o.schema = NewSchema(series)
	o.dropped = true

	return o.schema, nil
}

func (o *roundFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !o.loaded {
		o.loaded = true

		var err error
		if o.toNear, err = scalarValues(ctx, o.toArg, 1, o.ec.NumSteps()); err != nil {
			return nil, err
		}
	}

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)

	for i := range o.out.V {
		if !o.out.IsSet(i) {
			continue
		}

		to := o.toNear[i]

		// Upstream rounds half away from zero after scaling by the target precision.
		o.out.V[i] = math.Floor(o.out.V[i]*(1/to)+0.5) / (1 / to)
	}

	return &o.out, nil
}

// timestampFn replaces each sample's value with the step timestamp in seconds.
type timestampFn struct {
	input  Operator
	ec     *EvalContext
	schema *Schema
	out    Column
}

func newTimestampFn(input Operator, ec *EvalContext) *timestampFn {
	return &timestampFn{input: input, ec: ec}
}

func (o *timestampFn) String() string { return "Timestamp" }

func (o *timestampFn) Children() []Operator { return []Operator{o.input} }

func (o *timestampFn) Close() error { return o.input.Close() }

func (o *timestampFn) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	series := make([]labels.Labels, in.Len())
	for i, ls := range in.Series {
		series[i] = dropMetricName(ls)
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *timestampFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)
	for i := range o.out.V {
		if o.out.IsSet(i) {
			o.out.V[i] = float64(o.ec.Steps[i]) / 1000
		}
	}

	return &o.out, nil
}

// labelFn implements label_replace and label_join, which rewrite a series' identity. Both are
// pure schema transforms: values pass through untouched.
type labelFn struct {
	input   Operator
	name    string
	rewrite func(labels.Labels) (labels.Labels, error)

	schema *Schema
	// remap sends an input ref to its output ref; several inputs may collapse onto one output.
	remap []SeriesRef
	out   Column
}

func newLabelFn(input Operator, name string, rewrite func(labels.Labels) (labels.Labels, error)) *labelFn {
	return &labelFn{input: input, name: name, rewrite: rewrite}
}

func (o *labelFn) String() string { return fmt.Sprintf("LabelFn(%s)", o.name) }

func (o *labelFn) Children() []Operator { return []Operator{o.input} }

func (o *labelFn) Close() error { return o.input.Close() }

func (o *labelFn) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}

	var (
		series []labels.Labels
		byKey  = make(map[string]SeriesRef, in.Len())
	)

	o.remap = make([]SeriesRef, in.Len())

	for i, ls := range in.Series {
		out, err := o.rewrite(ls)
		if err != nil {
			return nil, err
		}

		key := out.String()

		ref, ok := byKey[key]
		if !ok {
			ref = SeriesRef(len(series))
			byKey[key] = ref
			series = append(series, out)
		}

		o.remap[i] = ref
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *labelFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)
	o.out.Ref = o.remap[col.Ref]

	return &o.out, nil
}

// vectorFn implements vector(s): a scalar promoted to a single label-less series.
type vectorFn struct {
	input Operator
	done  bool
	out   Column
}

func newVectorFn(input Operator) *vectorFn { return &vectorFn{input: input} }

func (o *vectorFn) String() string { return "Vector" }

func (o *vectorFn) Children() []Operator { return []Operator{o.input} }

func (o *vectorFn) Close() error { return o.input.Close() }

func (o *vectorFn) Schema(context.Context) (*Schema, error) {
	return NewSchema([]labels.Labels{labels.EmptyLabels()}), nil
}

func (o *vectorFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	col, err := o.input.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	o.out.CopyFrom(col)
	o.out.Ref = 0

	return &o.out, nil
}

// scalarFn implements scalar(v): the value of a single-series vector, or NaN when the input does
// not have exactly one series at that step.
type scalarFn struct {
	input Operator
	ec    *EvalContext
	done  bool
	out   Column
}

func newScalarFn(input Operator, ec *EvalContext) *scalarFn {
	return &scalarFn{input: input, ec: ec}
}

func (o *scalarFn) String() string { return "Scalar" }

func (o *scalarFn) Children() []Operator { return []Operator{o.input} }

func (o *scalarFn) Close() error { return o.input.Close() }

func (o *scalarFn) Schema(context.Context) (*Schema, error) { return ScalarSchema(), nil }

func (o *scalarFn) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	steps := o.ec.NumSteps()

	seen := make([]int, steps)
	vals := make([]float64, steps)

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return nil, err
		}

		if col == nil {
			break
		}

		for i := range steps {
			if col.IsSet(i) {
				seen[i]++
				vals[i] = col.V[i]
			}
		}
	}

	o.out.Resize(0, steps)

	for i := range steps {
		if seen[i] == 1 {
			o.out.Set(i, vals[i])
			continue
		}

		o.out.Set(i, math.NaN())
	}

	return &o.out, nil
}

// isScalarExpr reports whether an expression evaluates to a scalar.
func isScalarExpr(e parser.Expr) bool { return e.Type() == parser.ValueTypeScalar }
