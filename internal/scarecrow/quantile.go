package scarecrow

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// quantileAgg implements the `quantile` aggregation. Unlike [aggregate]'s incremental fold, an
// exact quantile needs every value at a (group, step) before it can answer any of them, so this
// operator retains the full per-step, per-group value set — O(groups × steps) cells, each
// growing to hold every series that lands in it. Upstream and Thanos pay the same cost; it is
// inherent to an exact quantile, not a layout artifact (see docs/promql-engine.md §4.4).
type quantileAgg struct {
	input    Operator
	grouping []string
	without  bool
	param    Operator // the quantile q, a per-step scalar
	ec       *EvalContext

	schema *Schema
	// groupOf maps an input series ref to its output group ref, same identity rule as [aggregate].
	groupOf []SeriesRef

	// cells holds every value seen for (group, step), indexed group*steps+step.
	cells [][]float64
	qVals []float64

	loaded bool
	cursor int
	out    Column
}

func newQuantileAgg(input, param Operator, e *parser.AggregateExpr, ec *EvalContext) *quantileAgg {
	return &quantileAgg{
		input:    input,
		grouping: e.Grouping,
		without:  e.Without,
		param:    param,
		ec:       ec,
	}
}

func (o *quantileAgg) String() string {
	return fmt.Sprintf("Quantile(by=%v, without=%v)", o.grouping, o.without)
}

func (o *quantileAgg) Children() []Operator { return []Operator{o.input, o.param} }

func (o *quantileAgg) Close() error { return o.input.Close() }

func (o *quantileAgg) Schema(ctx context.Context) (*Schema, error) {
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

	o.groupOf = make([]SeriesRef, in.Len())

	for i, ls := range in.Series {
		g := groupLabels(ls, o.grouping, o.without)

		key := g.String()

		ref, ok := byKey[key]
		if !ok {
			ref = SeriesRef(len(series))
			byKey[key] = ref
			series = append(series, g)
		}

		o.groupOf[i] = ref
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *quantileAgg) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	if _, err := o.Schema(ctx); err != nil {
		return err
	}

	steps := o.ec.NumSteps()

	var err error
	if o.qVals, err = scalarValues(ctx, o.param, math.NaN(), steps); err != nil {
		return err
	}

	o.cells = make([][]float64, o.schema.Len()*steps)

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			return nil
		}

		base := int(o.groupOf[col.Ref]) * steps
		for k := range steps {
			if !col.IsSet(k) {
				continue
			}

			o.cells[base+k] = append(o.cells[base+k], col.V[k])
		}
	}
}

func (o *quantileAgg) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	steps := o.ec.NumSteps()
	for o.cursor < o.schema.Len() {
		g := o.cursor
		o.cursor++

		o.out.Resize(SeriesRef(g), steps)

		base := g * steps
		for k := range steps {
			vals := o.cells[base+k]
			if vals == nil {
				continue
			}

			o.out.Set(k, quantile(o.qVals[k], vals))
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}

	return nil, nil
}

// quantile computes the q-quantile of values by linear interpolation between order statistics,
// matching upstream promql.quantile exactly (including its out-of-[0,1] and NaN conventions).
// It sorts values in place; callers must not reuse the slice afterward.
func quantile(q float64, values []float64) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}

	sort.Float64s(values)

	n := float64(len(values))
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)

	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}
