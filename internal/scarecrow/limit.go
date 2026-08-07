package scarecrow

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// limitEntry is one series' selected value at a (group, step) cell.
type limitEntry struct {
	ref SeriesRef // indexes the operator's *input* schema
	v   float64
}

// limitAgg implements topk, bottomk, limitk and limit_ratio: unlike [aggregate], their output
// identity is the *selected input series*, not a grouping label set, and unlike [quantileAgg]
// they need only the k survivors per (group, step) rather than the full set — a bounded heap,
// O(groups × k × steps) rather than O(groups × steps) growing per series (see
// docs/promql-engine.md §4.4).
//
// Grouping (`by`/`without`) here is internal bucketing only: it picks how many independent
// selections run, never the output label set, which is why groupOf is computed the same way as
// [aggregate] but never surfaces as a schema.
type limitAgg struct {
	input    Operator
	op       parser.ItemType // TOPK, BOTTOMK, LIMITK, LIMIT_RATIO
	grouping []string
	without  bool
	param    Operator // k (TOPK/BOTTOMK/LIMITK) or ratio (LIMIT_RATIO), a per-step scalar
	ec       *EvalContext

	inSchema  *Schema
	groupOf   []SeriesRef
	numGroups int

	// cells holds up to k selected entries per (group, step), indexed group*steps+step.
	// LIMIT_RATIO's selection is not k-bounded, so its cells grow to whatever the ratio admits.
	cells [][]limitEntry

	schema *Schema
	// outOf maps an *output* ref back to its input ref, so values are read from the right cell.
	outOf []SeriesRef
	// values/valid are the output's dense grid, row-major over (outRef, step).
	values []float64
	valid  []uint64

	loaded bool
	cursor int
	out    Column
}

func newLimitAgg(input, param Operator, e *parser.AggregateExpr, ec *EvalContext) *limitAgg {
	return &limitAgg{
		input:    input,
		op:       e.Op,
		grouping: e.Grouping,
		without:  e.Without,
		param:    param,
		ec:       ec,
	}
}

func (o *limitAgg) String() string {
	return fmt.Sprintf("Limit(%s, by=%v, without=%v)", o.op, o.grouping, o.without)
}

func (o *limitAgg) Children() []Operator { return []Operator{o.input, o.param} }

func (o *limitAgg) Close() error { return o.input.Close() }

// Schema runs the whole selection — which series survive is data-dependent, so like
// [aggregateOverTime] the work happens here rather than being deferred to the first Next.
func (o *limitAgg) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.schema, nil
}

func (o *limitAgg) resolveGroups(ctx context.Context) error {
	in, err := o.input.Schema(ctx)
	if err != nil {
		return err
	}

	o.inSchema = in

	var (
		byKey = make(map[string]SeriesRef, in.Len())
		next  SeriesRef
	)

	o.groupOf = make([]SeriesRef, in.Len())

	for i, ls := range in.Series {
		key := groupLabels(ls, o.grouping, o.without).String()

		ref, ok := byKey[key]
		if !ok {
			ref = next
			next++
			byKey[key] = ref
		}

		o.groupOf[i] = ref
	}

	o.numGroups = int(next)

	return nil
}

func (o *limitAgg) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	if err := o.resolveGroups(ctx); err != nil {
		return err
	}

	steps := o.ec.NumSteps()

	paramVals, err := scalarValues(ctx, o.param, 0, steps)
	if err != nil {
		return err
	}

	nanMsg := "Parameter value is NaN"
	if o.op == parser.LIMIT_RATIO {
		nanMsg = "Ratio value is NaN"
	}

	if slices.ContainsFunc(paramVals, math.IsNaN) {
		return errors.New(nanMsg)
	}

	o.cells = make([][]limitEntry, o.numGroups*steps)

	// LIMIT_RATIO's admission depends only on each series' label hash, not on its value, so it
	// is computed once per series rather than per (series, step).
	var offsets []float64
	if o.op == parser.LIMIT_RATIO {
		offsets = make([]float64, o.inSchema.Len())
		for i, ls := range o.inSchema.Series {
			offsets[i] = sampleOffset(ls)
		}
	}

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			break
		}

		g := int(o.groupOf[col.Ref])

		for k := range steps {
			if !col.IsSet(k) {
				continue
			}

			cell := &o.cells[g*steps+k]
			e := limitEntry{ref: col.Ref, v: col.V[k]}

			switch o.op {
			case parser.TOPK:
				insertBounded(cell, e, int(paramVals[k]), topkEvictable)
			case parser.BOTTOMK:
				insertBounded(cell, e, int(paramVals[k]), bottomkEvictable)
			case parser.LIMITK:
				kk := int(paramVals[k])
				if kk > 0 && len(*cell) < kk {
					*cell = append(*cell, e)
				}
			case parser.LIMIT_RATIO:
				r := clampRatio(paramVals[k])
				if addRatioSample(r, offsets[col.Ref]) {
					*cell = append(*cell, e)
				}
			}
		}
	}

	return o.buildSchema(steps)
}

// buildSchema resolves the output series set from every cell's survivors, and lays out the
// dense (outRef, step) grid those survivors are read into.
func (o *limitAgg) buildSchema(steps int) error {
	var (
		series []labels.Labels
		outRef = make(map[SeriesRef]SeriesRef, len(o.groupOf))
	)

	// Upstream emits topk/bottomk in value order — descending/ascending respectively — which
	// only an instant query (one step) can observe deterministically; a range query merges
	// steps by series identity regardless of order. Per-group, order by each survivor's first
	// appearance across the grid, since that is the only value order well-defined uniformly.
	less := func(_, _ limitEntry) bool { return false }
	switch o.op {
	case parser.TOPK:
		less = func(a, b limitEntry) bool { return a.v > b.v }
	case parser.BOTTOMK:
		less = func(a, b limitEntry) bool { return a.v < b.v }
	}

	for g := range o.numGroups {
		var cands []limitEntry

		seen := make(map[SeriesRef]bool)
		for step := range steps {
			for _, e := range o.cells[g*steps+step] {
				if seen[e.ref] {
					continue
				}

				seen[e.ref] = true
				cands = append(cands, e)
			}
		}

		if o.op == parser.TOPK || o.op == parser.BOTTOMK {
			slices.SortStableFunc(cands, func(a, b limitEntry) int {
				switch {
				case less(a, b):
					return -1
				case less(b, a):
					return 1
				default:
					return 0
				}
			})
		}

		for _, e := range cands {
			outRef[e.ref] = SeriesRef(len(series))
			series = append(series, o.inSchema.At(e.ref))
			o.outOf = append(o.outOf, e.ref)
		}
	}

	o.schema = NewSchema(series)

	n := len(series) * steps
	o.values = make([]float64, n)
	o.valid = make([]uint64, wordsFor(n))

	for step := range steps {
		for g := range o.numGroups {
			for _, e := range o.cells[g*steps+step] {
				i := int(outRef[e.ref])*steps + step
				o.values[i] = e.v
				setBit(o.valid, i)
			}
		}
	}

	return nil
}

func (o *limitAgg) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	steps := o.ec.NumSteps()
	for o.cursor < o.schema.Len() {
		ref := o.cursor
		o.cursor++

		o.out.Resize(SeriesRef(ref), steps)

		base := ref * steps
		for k := range steps {
			if !bitSet(o.valid, base+k) {
				continue
			}

			o.out.Set(k, o.values[base+k])
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}

	return nil, nil
}

// insertBounded keeps at most k entries in *cell, evicting the weakest kept entry — per
// evictable — when a new candidate arrives and the cell is already full. This is a linear-scan
// equivalent of upstream's bounded min/max heap: both always keep the true top/bottom-k set,
// and a heap's root is always the weakest kept entry by construction, so scanning for it here
// yields the same survivors. See docs/promql-engine.md §4.4.
func insertBounded(cell *[]limitEntry, e limitEntry, k int, evictable func(cur, cand float64) bool) {
	if k <= 0 {
		return
	}

	if len(*cell) < k {
		*cell = append(*cell, e)

		return
	}

	s := *cell

	worst := 0
	for i := 1; i < len(s); i++ {
		if evictable(s[i].v, s[worst].v) {
			worst = i
		}
	}

	if evictable(s[worst].v, e.v) {
		s[worst] = e
	}
}

// topkEvictable reports whether cur should be evicted in favor of cand: cur is smaller, or cur
// is NaN and cand is not. Matches upstream's `heap[0].F < s.F || (IsNaN(heap[0].F) && !IsNaN(s.F))`.
func topkEvictable(cur, cand float64) bool {
	return cur < cand || (math.IsNaN(cur) && !math.IsNaN(cand))
}

// bottomkEvictable is topkEvictable's mirror: cur is evicted when it is larger, or NaN.
func bottomkEvictable(cur, cand float64) bool {
	return cur > cand || (math.IsNaN(cur) && !math.IsNaN(cand))
}

// clampRatio bounds a limit_ratio parameter to [-1, 1], matching upstream's clamping (which also
// emits an annotation this engine does not yet produce — see M9).
func clampRatio(r float64) float64 {
	switch {
	case r > 1:
		return 1
	case r < -1:
		return -1
	default:
		return r
	}
}

// sampleOffset returns a deterministic value in [0, 1) derived from a series' label hash, per
// upstream's HashRatioSampler.
func sampleOffset(ls labels.Labels) float64 {
	const maxUint64 = float64(math.MaxUint64)

	return float64(ls.Hash()) / maxUint64
}

// addRatioSample reports whether offset falls within ratioLimit, per upstream's
// HashRatioSampler.AddRatioSampleWithOffset.
func addRatioSample(ratioLimit, offset float64) bool {
	return (ratioLimit >= 0 && offset < ratioLimit) ||
		(ratioLimit < 0 && offset >= 1.0+ratioLimit)
}
