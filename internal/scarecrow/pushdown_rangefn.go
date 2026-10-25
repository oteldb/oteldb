package scarecrow

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// RangeFunctionScanner evaluates supported range functions on a complete step grid.
// Returned samples contain final function values at grid timestamps, with metric names removed.
type RangeFunctionScanner interface {
	Scanner
	SupportsRangeFunction(name string) bool
	RangeFunction(ctx context.Context, name string, grid WindowGrid, matchers []*labels.Matcher) ([]Samples, error)
}

type rangeFunction struct {
	scanner  RangeFunctionScanner
	name     string
	matchers []*labels.Matcher
	refs     []int64
	width    int64
	ec       *EvalContext
	schema   *Schema
	values   []Samples
	next     int
	out      Column
}

func (p *planner) pushDownRangeFunction(name string, vs *parser.VectorSelector, ms *parser.MatrixSelector) (Operator, bool) {
	scanner, ok := p.scanner.(RangeFunctionScanner)
	if !ok || !scanner.SupportsRangeFunction(name) {
		return nil, false
	}
	return &rangeFunction{
		scanner: scanner, name: name, matchers: vs.LabelMatchers,
		refs:  refTimes(p.ec.Steps, vs.OriginalOffset, vs.Timestamp),
		width: ms.Range.Milliseconds(), ec: p.ec,
	}, true
}

func (o *rangeFunction) String() string       { return fmt.Sprintf("RangeFunction(%s)", o.name) }
func (o *rangeFunction) Children() []Operator { return nil }
func (o *rangeFunction) Close() error         { return nil }

func (o *rangeFunction) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}
	grid := WindowGrid{Start: o.refs[0], Step: 1, NumSteps: 1, Width: o.width}
	if len(o.refs) > 1 && o.refs[1] != o.refs[0] {
		grid.Step = o.refs[1] - o.refs[0]
		grid.NumSteps = len(o.refs)
	}
	values, err := o.scanner.RangeFunction(ctx, o.name, grid, o.matchers)
	if err != nil {
		return nil, errors.Wrap(err, "range function pushdown")
	}
	series := make([]labels.Labels, len(values))
	for i := range values {
		if len(values[i].T) != len(values[i].V) {
			return nil, errors.New("range function returned mismatched samples")
		}
		if err := o.ec.charge(len(values[i].T)); err != nil {
			return nil, err
		}
		series[i] = values[i].Labels
	}
	o.values = values
	o.schema = NewSchema(series)
	return o.schema, nil
}

func (o *rangeFunction) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}
	for o.next < len(o.values) {
		i := o.next
		o.next++
		o.out.Resize(SeriesRef(i), len(o.refs))
		values := &o.values[i]
		j := 0
		for step, ts := range o.refs {
			for j < len(values.T) && values.T[j] < ts {
				j++
			}
			if j < len(values.T) && values.T[j] == ts {
				o.out.Set(step, values.V[j])
			}
		}
		if !o.out.Empty() {
			return &o.out, nil
		}
	}
	return nil, nil
}
