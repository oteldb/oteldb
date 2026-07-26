package scarecrow

import (
	"context"
	"fmt"
	"math"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// changesMetricSchema reports whether an operator's result is no longer the input metric, so
// __name__ must be dropped. Comparisons keep the name (they filter rather than transform);
// arithmetic does not.
func changesMetricSchema(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.DIV, parser.MUL, parser.POW, parser.MOD, parser.ATAN2:
		return true
	default:
		return false
	}
}

// binopEval applies one binary operator to a pair of values. keep reports whether the sample
// survives, which is how filtering comparisons drop non-matching samples.
func binopEval(op parser.ItemType, l, r float64) (v float64, keep bool) {
	switch op {
	case parser.ADD:
		return l + r, true
	case parser.SUB:
		return l - r, true
	case parser.MUL:
		return l * r, true
	case parser.DIV:
		return l / r, true
	case parser.POW:
		return math.Pow(l, r), true
	case parser.MOD:
		return math.Mod(l, r), true
	case parser.ATAN2:
		return math.Atan2(l, r), true
	case parser.EQLC:
		return l, l == r
	case parser.NEQ:
		return l, l != r
	case parser.GTR:
		return l, l > r
	case parser.LSS:
		return l, l < r
	case parser.GTE:
		return l, l >= r
	case parser.LTE:
		return l, l <= r
	default:
		return math.NaN(), false
	}
}

// isComparison reports whether op is a filtering comparison.
func isComparison(op parser.ItemType) bool {
	switch op {
	case parser.EQLC, parser.NEQ, parser.GTR, parser.LSS, parser.GTE, parser.LTE:
		return true
	default:
		return false
	}
}

// scalarBinop combines two scalar operands into a scalar.
type scalarBinop struct {
	lhs, rhs Operator
	op       parser.ItemType
	returnB  bool
	ec       *EvalContext

	done bool
	out  Column
}

func newScalarBinop(lhs, rhs Operator, op parser.ItemType, returnBool bool, ec *EvalContext) *scalarBinop {
	return &scalarBinop{lhs: lhs, rhs: rhs, op: op, returnB: returnBool, ec: ec}
}

func (o *scalarBinop) String() string { return fmt.Sprintf("ScalarBinop(%s)", o.op) }

func (o *scalarBinop) Children() []Operator { return []Operator{o.lhs, o.rhs} }

func (o *scalarBinop) Close() error { return errors.Join(o.lhs.Close(), o.rhs.Close()) }

func (o *scalarBinop) Schema(context.Context) (*Schema, error) { return ScalarSchema(), nil }

func (o *scalarBinop) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	l, err := o.lhs.Next(ctx)
	if err != nil {
		return nil, err
	}

	r, err := o.rhs.Next(ctx)
	if err != nil {
		return nil, err
	}

	o.out.Resize(0, o.ec.NumSteps())

	if l == nil || r == nil {
		return &o.out, nil
	}

	for i := range o.out.V {
		if !l.IsSet(i) || !r.IsSet(i) {
			continue
		}

		v, keep := binopEval(o.op, l.V[i], r.V[i])

		// Between two scalars a comparison always yields a value; without bool it is the
		// left-hand value when the comparison holds and NaN when it does not.
		switch {
		case !isComparison(o.op):
			o.out.Set(i, v)
		case o.returnB:
			o.out.Set(i, boolValue(keep))
		case keep:
			o.out.Set(i, v)
		default:
			o.out.Set(i, math.NaN())
		}
	}

	return &o.out, nil
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// vectorScalarBinop applies a scalar operand to every series of a vector, streaming: one input
// column in, one output column out.
type vectorScalarBinop struct {
	vector Operator
	scalar Operator
	op     parser.ItemType
	// swap reports that the scalar was the left-hand operand.
	swap    bool
	returnB bool

	ec      *EvalContext
	schema  *Schema
	scalars []float64
	loaded  bool
	out     Column
}

func newVectorScalarBinop(
	vector, scalar Operator, op parser.ItemType, swap, returnBool bool, ec *EvalContext,
) *vectorScalarBinop {
	return &vectorScalarBinop{vector: vector, scalar: scalar, op: op, swap: swap, returnB: returnBool, ec: ec}
}

func (o *vectorScalarBinop) String() string { return fmt.Sprintf("VectorScalarBinop(%s)", o.op) }

func (o *vectorScalarBinop) Children() []Operator { return []Operator{o.vector, o.scalar} }

func (o *vectorScalarBinop) Close() error {
	return errors.Join(o.vector.Close(), o.scalar.Close())
}

func (o *vectorScalarBinop) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	in, err := o.vector.Schema(ctx)
	if err != nil {
		return nil, err
	}

	// Arithmetic and bool comparisons produce a different metric; a filtering comparison keeps
	// the input series identity intact.
	if !changesMetricSchema(o.op) && !o.returnB {
		o.schema = in

		return o.schema, nil
	}

	series := make([]labels.Labels, in.Len())
	for i, ls := range in.Series {
		series[i] = dropMetricName(ls)
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *vectorScalarBinop) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !o.loaded {
		o.loaded = true

		vals, err := scalarValues(ctx, o.scalar, math.NaN(), o.ec.NumSteps())
		if err != nil {
			return nil, err
		}
		o.scalars = vals
	}

	for {
		col, err := o.vector.Next(ctx)
		if err != nil || col == nil {
			return nil, err
		}

		o.out.CopyFrom(col)

		for i := range o.out.V {
			if !o.out.IsSet(i) {
				continue
			}

			l, r := o.out.V[i], o.scalars[i]
			if o.swap {
				l, r = r, l
			}

			v, keep := binopEval(o.op, l, r)

			// A comparison always reports the vector element's value, even when the scalar was
			// the left operand and binopEval therefore returned the scalar.
			if isComparison(o.op) && o.swap {
				v = r
			}

			switch {
			case o.returnB:
				o.out.V[i] = boolValue(keep)
			case !keep:
				o.out.Clear(i)
			default:
				o.out.V[i] = v
			}
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}
}

// signature returns a series' vector-matching key: the labels named by on(), or every label
// except those named by ignoring() and __name__.
func signature(ls labels.Labels, matching *parser.VectorMatching) uint64 {
	if matching == nil {
		return dropMetricName(ls).Hash()
	}

	if matching.On {
		return labels.NewBuilder(ls).Keep(matching.MatchingLabels...).Labels().Hash()
	}

	lb := labels.NewBuilder(ls)
	lb.Del(matching.MatchingLabels...)
	lb.Del(model.MetricNameLabel)

	return lb.Labels().Hash()
}

// resultLabels computes a binop result's identity, following upstream: start from the "many"
// side, drop __name__ when the operator changes the metric, narrow by on()/ignoring() for
// one-to-one, then graft the group_x include labels from the "one" side.
func resultLabels(many, one labels.Labels, op parser.ItemType, matching *parser.VectorMatching, dropName bool) labels.Labels {
	lb := labels.NewBuilder(many)

	if dropName || changesMetricSchema(op) {
		lb.Del(model.MetricNameLabel)
	}

	if matching.Card == parser.CardOneToOne {
		if matching.On {
			lb.Keep(matching.MatchingLabels...)
		} else {
			lb.Del(matching.MatchingLabels...)
		}
	}

	for _, ln := range matching.Include {
		if v := one.Get(ln); v != "" {
			lb.Set(ln, v)
		} else {
			lb.Del(ln)
		}
	}

	return lb.Labels()
}

// vectorBinop combines two vectors under a matching rule.
//
// The "one" side is materialized into a signature-keyed table (the build side) and the "many"
// side streams against it, which keeps resident memory at O(oneSideSeries × steps) rather than
// holding both inputs.
type vectorBinop struct {
	lhs, rhs Operator
	op       parser.ItemType
	matching *parser.VectorMatching
	returnB  bool
	ec       *EvalContext

	// After normalization many is the streaming side and one is the build side. swapped records
	// that the original expression was one-to-many, so operand order is restored per sample.
	many, one Operator
	swapped   bool

	schema *Schema
	// pairOf maps a many-side ref to its output ref, or -1 when it matches nothing.
	pairOf []int
	// oneOf maps a many-side ref to the one-side ref it matched.
	oneOf []SeriesRef

	built   bool
	oneCols map[SeriesRef]*Column
	// bySig maps a signature to the one-side ref carrying it.
	bySig map[uint64]SeriesRef

	out Column
}

func newVectorBinop(
	lhs, rhs Operator, op parser.ItemType, matching *parser.VectorMatching, returnBool bool, ec *EvalContext,
) *vectorBinop {
	o := &vectorBinop{lhs: lhs, rhs: rhs, op: op, matching: matching, returnB: returnBool, ec: ec}

	// Upstream swaps sidedness for one-to-many so the "one" side is always on the right.
	o.many, o.one = lhs, rhs
	if matching.Card == parser.CardOneToMany {
		o.many, o.one = rhs, lhs
		o.swapped = true
	}

	return o
}

func (o *vectorBinop) String() string { return fmt.Sprintf("VectorBinop(%s)", o.op) }

func (o *vectorBinop) Children() []Operator { return []Operator{o.lhs, o.rhs} }

func (o *vectorBinop) Close() error { return errors.Join(o.lhs.Close(), o.rhs.Close()) }

func (o *vectorBinop) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	manySchema, err := o.many.Schema(ctx)
	if err != nil {
		return nil, err
	}

	oneSchema, err := o.one.Schema(ctx)
	if err != nil {
		return nil, err
	}

	o.bySig = make(map[uint64]SeriesRef, oneSchema.Len())
	for i, ls := range oneSchema.Series {
		sig := signature(ls, o.matching)
		if _, dup := o.bySig[sig]; dup && o.matching.Card != parser.CardOneToOne {
			return nil, errors.Errorf(
				"found duplicate series for the match group on the one hand-side of the operation: %s", ls,
			)
		}

		o.bySig[sig] = SeriesRef(i)
	}

	var series []labels.Labels

	o.pairOf = make([]int, manySchema.Len())
	o.oneOf = make([]SeriesRef, manySchema.Len())

	for i, ls := range manySchema.Series {
		oneRef, ok := o.bySig[signature(ls, o.matching)]
		if !ok {
			o.pairOf[i] = -1
			continue
		}

		o.pairOf[i] = len(series)
		o.oneOf[i] = oneRef

		series = append(series, resultLabels(ls, oneSchema.At(oneRef), o.op, o.matching, o.returnB))
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

// build materializes the one side, which must be resident for the many side to probe it.
func (o *vectorBinop) build(ctx context.Context) error {
	o.built = true
	o.oneCols = make(map[SeriesRef]*Column)

	for {
		col, err := o.one.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			return nil
		}

		// The producer owns its column and will overwrite it, so the build side keeps a copy.
		kept := NewColumn(col.Ref, col.Steps())
		kept.CopyFrom(col)
		o.oneCols[col.Ref] = kept
	}
}

func (o *vectorBinop) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	if !o.built {
		if err := o.build(ctx); err != nil {
			return nil, err
		}
	}

	for {
		col, err := o.many.Next(ctx)
		if err != nil || col == nil {
			return nil, err
		}

		outRef := o.pairOf[col.Ref]
		if outRef < 0 {
			continue // no matching series on the one side
		}

		oneCol, ok := o.oneCols[o.oneOf[col.Ref]]
		if !ok {
			continue // the matched series produced no samples
		}

		o.out.Resize(SeriesRef(outRef), col.Steps())

		for i := range col.V {
			if !col.IsSet(i) || !oneCol.IsSet(i) {
				continue
			}

			l, r := col.V[i], oneCol.V[i]
			if o.swapped {
				l, r = r, l
			}

			v, keep := binopEval(o.op, l, r)

			switch {
			case o.returnB:
				o.out.Set(i, boolValue(keep))
			case keep:
				o.out.Set(i, v)
			}
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}
}

// setBinop implements and, or and unless, which match on signature but take their values from
// one side only.
type setBinop struct {
	lhs, rhs Operator
	op       parser.ItemType
	matching *parser.VectorMatching
	ec       *EvalContext

	schema *Schema
	// lhsOut maps an lhs ref to its output ref; rhsOut likewise, used only by `or`.
	lhsOut, rhsOut []int
	// lhsSig and rhsSig hold each side's matching signature by ref.
	lhsSig, rhsSig []uint64

	built bool
	// result holds one accumulator column per output series, indexed by output ref.
	result []*Column

	cursor int
}

func newSetBinop(lhs, rhs Operator, op parser.ItemType, matching *parser.VectorMatching, ec *EvalContext) *setBinop {
	return &setBinop{lhs: lhs, rhs: rhs, op: op, matching: matching, ec: ec}
}

func (o *setBinop) String() string { return fmt.Sprintf("SetBinop(%s)", o.op) }

func (o *setBinop) Children() []Operator { return []Operator{o.lhs, o.rhs} }

func (o *setBinop) Close() error { return errors.Join(o.lhs.Close(), o.rhs.Close()) }

func (o *setBinop) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	l, err := o.lhs.Schema(ctx)
	if err != nil {
		return nil, err
	}

	r, err := o.rhs.Schema(ctx)
	if err != nil {
		return nil, err
	}

	// Set operators preserve series identity, so the output is the lhs (plus, for `or`, the rhs
	// series that are not already present).
	series := make([]labels.Labels, 0, l.Len())
	byKey := make(map[string]int, l.Len())

	o.lhsOut = make([]int, l.Len())
	o.lhsSig = make([]uint64, l.Len())

	for i, ls := range l.Series {
		o.lhsSig[i] = signature(ls, o.matching)
		o.lhsOut[i] = len(series)
		byKey[ls.String()] = o.lhsOut[i]
		series = append(series, ls)
	}

	o.rhsOut = make([]int, r.Len())
	o.rhsSig = make([]uint64, r.Len())

	for i, ls := range r.Series {
		o.rhsSig[i] = signature(ls, o.matching)
		o.rhsOut[i] = -1

		if o.op != parser.LOR {
			continue
		}

		if ref, ok := byKey[ls.String()]; ok {
			o.rhsOut[i] = ref
			continue
		}

		o.rhsOut[i] = len(series)
		byKey[ls.String()] = o.rhsOut[i]
		series = append(series, ls)
	}

	o.schema = NewSchema(series)

	return o.schema, nil
}

func (o *setBinop) build(ctx context.Context) error {
	o.built = true

	steps := o.ec.NumSteps()
	rhsSteps := make(map[uint64][]uint64)
	lhsSteps := make(map[uint64][]uint64)

	markSteps := func(m map[uint64][]uint64, sig uint64, col *Column) {
		w, ok := m[sig]
		if !ok {
			w = make([]uint64, wordsFor(steps))
			m[sig] = w
		}

		for i := range w {
			w[i] |= col.Valid[i]
		}
	}

	// Both sides are buffered: `or` decides per step whether an rhs sample is suppressed, and
	// that is only known once every lhs series has been seen.
	var rhsBuf, lhsBuf []*Column

	for {
		col, err := o.rhs.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			break
		}

		markSteps(rhsSteps, o.rhsSig[col.Ref], col)

		if o.op == parser.LOR {
			kept := NewColumn(col.Ref, col.Steps())
			kept.CopyFrom(col)
			rhsBuf = append(rhsBuf, kept)
		}
	}

	for {
		col, err := o.lhs.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			break
		}

		markSteps(lhsSteps, o.lhsSig[col.Ref], col)

		kept := NewColumn(col.Ref, col.Steps())
		kept.CopyFrom(col)
		lhsBuf = append(lhsBuf, kept)
	}

	// Accumulate into one column per output series. Distinct inputs can collapse onto the same
	// identity — `-a or -b` both reduce to {} — so `or` must merge them rather than emit twice.
	o.result = make([]*Column, o.schema.Len())

	for _, col := range lhsBuf {
		out := o.outputFor(SeriesRef(o.lhsOut[col.Ref]), steps)
		match := rhsSteps[o.lhsSig[col.Ref]]

		for i := range steps {
			if !col.IsSet(i) {
				continue
			}

			present := match != nil && bitSet(match, i)

			// `and` keeps a step only where the rhs has one, `unless` only where it does not,
			// and `or` keeps every lhs step.
			switch o.op {
			case parser.LAND:
				if !present {
					continue
				}
			case parser.LUNLESS:
				if present {
					continue
				}
			}

			out.Set(i, col.V[i])
		}
	}

	for _, col := range rhsBuf {
		out := o.outputFor(SeriesRef(o.rhsOut[col.Ref]), steps)
		lhsMatch := lhsSteps[o.rhsSig[col.Ref]]

		for i := range steps {
			// An rhs sample survives only where no lhs series shares its signature, and never
			// overwrites a value already contributed by the lhs.
			if !col.IsSet(i) || out.IsSet(i) {
				continue
			}

			if lhsMatch != nil && bitSet(lhsMatch, i) {
				continue
			}

			out.Set(i, col.V[i])
		}
	}

	return nil
}

// outputFor returns the accumulator column for an output ref, creating it on first use.
func (o *setBinop) outputFor(ref SeriesRef, steps int) *Column {
	if c := o.result[ref]; c != nil {
		return c
	}

	c := NewColumn(ref, steps)
	o.result[ref] = c

	return c
}

func (o *setBinop) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	if !o.built {
		if err := o.build(ctx); err != nil {
			return nil, err
		}
	}

	for o.cursor < len(o.result) {
		col := o.result[o.cursor]
		o.cursor++

		if col == nil || col.Empty() {
			continue
		}

		return col, nil
	}

	return nil, nil
}
