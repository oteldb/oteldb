package scarecrow

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/facette/natsort"
	"github.com/prometheus/prometheus/model/labels"
)

// sortOp implements sort, sort_desc, sort_by_label and sort_by_label_desc.
//
// These functions only reorder emission, so they cost nothing at any step but the first: the
// order is fixed once, from the child's first (and, per docs/promql-engine.md §4.4, only
// meaningful) step, by resolving the child's whole series set up front and permuting the
// schema — Next then streams straight through in that fixed order. This still needs the full
// input resident at once, the same O(series × steps) cost as [quantileAgg], because the order
// cannot be known until every candidate key has been seen.
type sortOp struct {
	input Operator
	// byValue ranks by the value at the child's first step; nil for the label-ranked forms.
	byValue func(a, b float64) bool
	// byLabel ranks by the named labels' values, falling back to the full label set; nil for
	// the value-ranked forms.
	byLabel []string
	desc    bool
	ec      *EvalContext

	schema *Schema
	// order[i] is the input ref that should be emitted i-th.
	order []SeriesRef
	// cols holds every input column, indexed by input ref, so Next can stream them out in
	// `order` once the ranking is known.
	cols []Column

	loaded bool
	cursor int
}

func newSortByValue(input Operator, desc bool, ec *EvalContext) *sortOp {
	fn := ascendingNaNLast
	if desc {
		fn = descendingNaNLast
	}

	return &sortOp{input: input, byValue: fn, desc: desc, ec: ec}
}

func newSortByLabel(input Operator, labelNames []string, desc bool, ec *EvalContext) *sortOp {
	return &sortOp{input: input, byLabel: labelNames, desc: desc, ec: ec}
}

func (o *sortOp) String() string {
	if o.byLabel != nil {
		return fmt.Sprintf("Sort(by=%v, desc=%v)", o.byLabel, o.desc)
	}

	return fmt.Sprintf("Sort(desc=%v)", o.desc)
}

func (o *sortOp) Children() []Operator { return []Operator{o.input} }

func (o *sortOp) Close() error { return o.input.Close() }

// Schema resolves the emission order, which needs the input drained — see the type doc.
func (o *sortOp) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.schema, nil
}

func (o *sortOp) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	in, err := o.input.Schema(ctx)
	if err != nil {
		return err
	}

	o.cols = make([]Column, in.Len())
	o.order = make([]SeriesRef, 0, in.Len())

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			break
		}

		o.cols[col.Ref].CopyFrom(col)
		o.order = append(o.order, col.Ref)
	}

	switch {
	case o.byLabel != nil:
		slices.SortFunc(o.order, func(a, b SeriesRef) int {
			return compareByLabel(in.At(a), in.At(b), o.byLabel, o.desc)
		})
	default:
		slices.SortFunc(o.order, func(a, b SeriesRef) int {
			av, bv := firstValue(&o.cols[a]), firstValue(&o.cols[b])

			switch {
			case o.byValue(av, bv):
				return -1
			case o.byValue(bv, av):
				return 1
			default:
				return 0
			}
		})
	}

	series := make([]labels.Labels, len(o.order))
	for i, ref := range o.order {
		series[i] = in.At(ref)
	}

	o.schema = NewSchema(series)

	return nil
}

func (o *sortOp) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	if o.cursor >= len(o.order) {
		return nil, nil
	}

	ref := o.order[o.cursor]
	o.cursor++

	col := &o.cols[ref]
	col.Ref = SeriesRef(o.cursor - 1)

	return col, nil
}

// firstValue returns a column's value at step 0, or NaN if it has none — the key sort/sort_desc
// rank by, since they are only specified for instant queries (a one-step grid).
func firstValue(c *Column) float64 {
	if c.Steps() == 0 || !c.IsSet(0) {
		return math.NaN()
	}

	return c.V[0]
}

// ascendingNaNLast and descendingNaNLast report whether a ranks strictly before b, with NaN
// forced to the bottom either way — the rule sort()/sort_desc() specify, reached here directly
// rather than through upstream's double-reverse heap trick.
func ascendingNaNLast(a, b float64) bool {
	switch {
	case math.IsNaN(a):
		return false
	case math.IsNaN(b):
		return true
	default:
		return a < b
	}
}

func descendingNaNLast(a, b float64) bool {
	switch {
	case math.IsNaN(a):
		return false
	case math.IsNaN(b):
		return true
	default:
		return a > b
	}
}

// compareByLabel implements sort_by_label(_desc)'s ordering: rank by each named label's value in
// turn, using natural (digit-aware) string comparison, falling back to the full label set so the
// order is total.
func compareByLabel(a, b labels.Labels, names []string, desc bool) int {
	for _, name := range names {
		av, bv := a.Get(name), b.Get(name)
		if av == bv {
			continue
		}

		c := -1
		if !natsort.Compare(av, bv) {
			c = 1
		}

		if desc {
			c = -c
		}

		return c
	}

	c := labels.Compare(a, b)
	if desc {
		c = -c
	}

	return c
}
