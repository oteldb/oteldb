package scarecrow

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/kahansum"
)

// aggregate folds many input series into one row per group.
//
// It is the archetypal accumulating operator, and the reason series-major execution pays off:
// each input column is added into its group's row and released, so resident memory is
// O(groups × steps) rather than O(series × steps). Nothing here ever holds the input set.
type aggregate struct {
	input    Operator
	op       parser.ItemType
	grouping []string
	without  bool
	ec       *EvalContext

	schema *Schema
	// groupOf maps an input series ref to its output group ref.
	groupOf []SeriesRef

	// Accumulator rows, each groups×steps, indexed by group*steps + step.
	sum   []float64
	count []float64
	mean  []float64
	m2    []float64
	valid []uint64
	// kahanC carries the compensation term of the running sum (and of avg's running mean).
	// Upstream sums this way, and several corpus cases exist precisely to catch a naive sum.
	kahanC []float64
	// incMean marks the groups where avg switched from a direct mean to an incremental one,
	// which upstream does once the running sum would overflow.
	incMean []bool

	drained bool
	cursor  int
	out     Column
}

func newAggregate(input Operator, e *parser.AggregateExpr, ec *EvalContext) *aggregate {
	return &aggregate{
		input:    input,
		op:       e.Op,
		grouping: e.Grouping,
		without:  e.Without,
		ec:       ec,
	}
}

func (o *aggregate) String() string {
	return fmt.Sprintf("Aggregate(%s, by=%v, without=%v)", o.op, o.grouping, o.without)
}

func (o *aggregate) Children() []Operator { return []Operator{o.input} }

func (o *aggregate) Close() error { return o.input.Close() }

// groupLabels reduces a series' labels to its group identity. `by` keeps only the listed
// labels; `without` drops them along with __name__. Either way the result is no longer the
// input metric, so __name__ never survives.
func groupLabels(ls labels.Labels, grouping []string, without bool) labels.Labels {
	lb := labels.NewBuilder(ls)

	if without {
		lb.Del(grouping...)
		lb.Del(model.MetricNameLabel)
	} else {
		lb.Keep(grouping...)
	}

	return lb.Labels()
}

func (o *aggregate) Schema(ctx context.Context) (*Schema, error) {
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

func (o *aggregate) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !o.drained {
		if err := o.drain(ctx); err != nil {
			return nil, err
		}
	}

	steps := o.ec.NumSteps()
	for o.cursor < o.schema.Len() {
		g := o.cursor
		o.cursor++

		o.out.Resize(SeriesRef(g), steps)

		base := g * steps
		for k := range steps {
			if !bitSet(o.valid, base+k) {
				continue
			}

			o.out.Set(k, o.finalize(base+k))
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}

	return nil, nil
}

// drain consumes the whole input, accumulating each column into its group's row.
func (o *aggregate) drain(ctx context.Context) error {
	o.drained = true

	if _, err := o.Schema(ctx); err != nil {
		return err
	}

	steps := o.ec.NumSteps()
	n := o.schema.Len() * steps

	o.sum = make([]float64, n)
	o.count = make([]float64, n)
	o.valid = make([]uint64, wordsFor(n))

	switch o.op {
	case parser.SUM:
		o.kahanC = make([]float64, n)
	case parser.AVG:
		o.mean = make([]float64, n)
		o.kahanC = make([]float64, n)
		o.incMean = make([]bool, n)
	case parser.STDDEV, parser.STDVAR:
		o.mean = make([]float64, n)
		o.m2 = make([]float64, n)
	}

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

			o.accumulate(base+k, col.V[k])
		}
	}
}

func (o *aggregate) accumulate(i int, v float64) {
	first := !bitSet(o.valid, i)
	setBit(o.valid, i)

	o.count[i]++

	switch o.op {
	case parser.SUM:
		o.sum[i], o.kahanC[i] = kahansum.Inc(v, o.sum[i], o.kahanC[i])

	case parser.MIN:
		if first || v < o.sum[i] || math.IsNaN(o.sum[i]) {
			o.sum[i] = v
		}

	case parser.MAX:
		if first || v > o.sum[i] || math.IsNaN(o.sum[i]) {
			o.sum[i] = v
		}

	case parser.AVG:
		o.avg(i, v)

	case parser.STDDEV, parser.STDVAR:
		delta := v - o.mean[i]
		o.mean[i] += delta / o.count[i]
		o.m2[i] += delta * (v - o.mean[i])
	}
}

// avg folds v into group slot i's running average.
//
// A direct Kahan-compensated sum divided by the count is more accurate than an incremental
// mean, but it overflows float64 on inputs an incremental mean handles fine. Upstream therefore
// sums directly until the sum *would* overflow and only then switches that group to an
// incremental mean; this mirrors that, because the corpus tests both regimes.
func (o *aggregate) avg(i int, v float64) {
	count := o.count[i]

	// The first value seeds the sum directly. Folding it would divide by count-1 == 0 in the
	// overflow branch below, turning an infinite first sample into NaN.
	if count == 1 {
		o.sum[i], o.kahanC[i] = v, 0

		return
	}

	if !o.incMean[i] {
		newV, newC := kahansum.Inc(v, o.sum[i], o.kahanC[i])
		if !math.IsInf(newV, 0) {
			o.sum[i], o.kahanC[i] = newV, newC

			return
		}

		// The sum would overflow, so fall back to an incremental mean from here on.
		o.incMean[i] = true
		o.mean[i] = o.sum[i] / (count - 1)
		o.kahanC[i] /= count - 1
	}

	// An infinite running mean cannot take part in the incremental update, which subtracts the
	// mean and would turn Inf += x - Inf into NaN. In both these cases the mean is already
	// correct, so it is left alone.
	if math.IsInf(o.mean[i], 0) {
		switch {
		case math.IsInf(v, 0) && (v > 0) == (o.mean[i] > 0):
			return // same-signed infinity
		case !math.IsInf(v, 0) && !math.IsNaN(v):
			return // a finite value cannot pull an infinite mean back
		}
	}

	q := (count - 1) / count
	o.mean[i], o.kahanC[i] = kahansum.Inc(v/count, q*o.mean[i], q*o.kahanC[i])
}

func (o *aggregate) finalize(i int) float64 {
	switch o.op {
	case parser.SUM:
		return o.sum[i] + o.kahanC[i]
	case parser.MIN, parser.MAX:
		return o.sum[i]
	case parser.COUNT:
		return o.count[i]
	case parser.GROUP:
		return 1
	case parser.AVG:
		if o.incMean[i] {
			return o.mean[i] + o.kahanC[i]
		}

		return (o.sum[i] + o.kahanC[i]) / o.count[i]
	case parser.STDVAR:
		return o.m2[i] / o.count[i]
	case parser.STDDEV:
		return math.Sqrt(o.m2[i] / o.count[i])
	default:
		return math.NaN()
	}
}

func bitSet(w []uint64, i int) bool { return w[i>>6]&(1<<uint(i&63)) != 0 }

func setBit(w []uint64, i int) { w[i>>6] |= 1 << uint(i&63) }
