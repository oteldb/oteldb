package scarecrow

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// createLabelsForAbsentFunction derives absent()/absent_over_time()'s output identity from the
// argument's own label matchers rather than from any series it observes — the sole scarecrow
// operator whose output labels come from AST syntax, not from data. Only equality matchers
// contribute, and a label used twice (`x{job="a",job="b"}`) is dropped rather than resolved
// arbitrarily, matching upstream's documented backwards-compatibility quirk.
func createLabelsForAbsentFunction(expr parser.Expr) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())

	var lm []*labels.Matcher
	switch n := expr.(type) {
	case *parser.VectorSelector:
		lm = n.LabelMatchers
	case *parser.MatrixSelector:
		vs, ok := n.VectorSelector.(*parser.VectorSelector)
		if !ok {
			return labels.EmptyLabels()
		}

		lm = vs.LabelMatchers
	default:
		return labels.EmptyLabels()
	}

	has := make(map[string]bool, len(lm))
	for _, m := range lm {
		if m.Name == model.MetricNameLabel {
			continue
		}

		if m.Type == labels.MatchEqual && !has[m.Name] {
			b.Set(m.Name, m.Value)
			has[m.Name] = true
		} else {
			b.Del(m.Name)
		}
	}

	return b.Labels()
}

// absentOp implements both absent() and absent_over_time(): it emits exactly one series, whose
// identity is fixed at plan time from the argument's own matchers (via
// [createLabelsForAbsentFunction]), carrying a sample of 1 at every step where the child produced
// nothing at all. This is the one operator whose data-dependence is fully resolved by observing
// presence, never a value — Schema is trivial and known before any Next call; only Next needs the
// child, and it needs all of it, so this is an ordinary accumulating operator (§4.2), not a
// Schema()-time drain like [quantileAgg] or [countValuesAgg].
//
// absent_over_time reuses this unchanged: its child is presentOverTime (an ordinary range
// function marking, per series, whether *that* series' window held a sample), and OR-reducing
// that across the whole matched set per step is exactly what this type already does for absent()'s
// child.
type absentOp struct {
	input  Operator
	labels labels.Labels
	ec     *EvalContext

	schema *Schema
	out    Column
	done   bool
}

func newAbsentOp(input Operator, ls labels.Labels, ec *EvalContext) *absentOp {
	return &absentOp{input: input, labels: ls, ec: ec}
}

func (o *absentOp) String() string { return fmt.Sprintf("Absent(%s)", o.labels) }

func (o *absentOp) Children() []Operator { return []Operator{o.input} }

func (o *absentOp) Close() error { return o.input.Close() }

func (o *absentOp) Schema(context.Context) (*Schema, error) {
	if o.schema == nil {
		o.schema = NewSchema([]labels.Labels{o.labels})
	}

	return o.schema, nil
}

func (o *absentOp) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	steps := o.ec.NumSteps()
	present := make([]uint64, wordsFor(steps))

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return nil, err
		}

		if col == nil {
			break
		}

		for k := range steps {
			if col.IsSet(k) {
				setBit(present, k)
			}
		}
	}

	o.out.Resize(0, steps)
	for k := range steps {
		if !bitSet(present, k) {
			o.out.Set(k, 1)
		}
	}

	if o.out.Empty() {
		return nil, nil
	}

	return &o.out, nil
}

// buildAbsentOverTime plans absent_over_time(). It cannot reuse the normal rangeFunc/matrixFold
// contract: [matrixFold] never calls a range function over an empty window, but an empty window
// is exactly the condition this needs to observe. Instead it plans presentOverTime as an
// ordinary range-vector fold (still eligible for the same pushdown every other *_over_time
// function gets) and wraps it in [absentOp], which OR-reduces presence across the whole matched
// set per step — the same trick absent() uses over its own child.
func (p *planner) buildAbsentOverTime(e *parser.Call) (Operator, error) {
	if len(e.Args) != 1 {
		return nil, unsupportedf("absent_over_time with %d arguments", len(e.Args))
	}

	presentFn := rangeFuncs["present_over_time"]

	switch arg := e.Args[0].(type) {
	case *parser.MatrixSelector:
		input, err := p.buildSelectorFold("present_over_time", presentFn, arg, nil)
		if err != nil {
			return nil, err
		}

		return newAbsentOp(input, createLabelsForAbsentFunction(arg), p.ec), nil

	case *parser.SubqueryExpr:
		input, err := p.buildSubqueryFold("present_over_time", presentFn, arg, nil)
		if err != nil {
			return nil, err
		}

		return newAbsentOp(input, labels.EmptyLabels(), p.ec), nil

	default:
		return nil, unsupportedf("absent_over_time over %T", e.Args[0])
	}
}
