package scarecrow

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
)

// aggregateOverTime answers a reducer `*_over_time` from an [AggregateScanner] instead of folding
// raw samples, replacing [matrixFold] for the functions in [overTimeFolds].
//
// What it saves is the raw level: [matrixFold] pulls every sample in every step's window across
// the seam and folds it here, which is what makes the fork's matrix selector the second-largest
// live allocation in production (#1117). This ships one aggregate per (series, step) instead.
//
// What it costs is honest to state: storage answers a *window at a time*, all series at once,
// while this engine emits a *series at a time*, all steps at once. So the results of every
// window are held until the last one lands — O(series × steps) floats, against the O(1) in
// series that a streaming selector achieves (§4.6). It is still far less than the raw samples it
// replaces (a 5m window at 15s holds 20 samples per series per step, and the aggregate holds
// one), and time-chunking bounds it further, but it is the one place in the engine where result
// memory scales with the matched series set. A series-major aggregate API on the storage side
// would remove it; see the note in docs/promql-engine.md §5.2.
type aggregateOverTime struct {
	scanner AggregateScanner
	// grid, when non-nil, folds every step's window in one call instead of one call per step.
	// See [GridAggregateScanner]: the per-window seam is a pessimization at range-query scale.
	grid     GridAggregateScanner
	matchers []*labels.Matcher
	label    string
	fnName   string
	fold     func(WindowAggregate) float64

	rng    time.Duration
	offset time.Duration
	at     *int64
	ec     *EvalContext

	schema *Schema
	byHash map[uint64][]SeriesRef

	// values and valid are row-major over (ref, step): ref*steps + step.
	values []float64
	valid  []bool
	loaded bool

	next SeriesRef
	out  Column
}

func newAggregateOverTime(
	scanner AggregateScanner,
	grid GridAggregateScanner,
	matchers []*labels.Matcher,
	label, fnName string,
	fold func(WindowAggregate) float64,
	rng, offset time.Duration,
	at *int64,
	ec *EvalContext,
) *aggregateOverTime {
	return &aggregateOverTime{
		scanner:  scanner,
		grid:     grid,
		matchers: matchers,
		label:    label,
		fnName:   fnName,
		fold:     fold,
		rng:      rng,
		offset:   offset,
		at:       at,
		ec:       ec,
	}
}

func (o *aggregateOverTime) String() string {
	return fmt.Sprintf("AggregateOverTime(%s, %s[%s])", o.fnName, o.label, o.rng)
}

func (o *aggregateOverTime) Children() []Operator { return nil }

func (o *aggregateOverTime) Close() error { return nil }

// Schema runs the pushdown. Every window must be folded before the series set is known — a
// series can appear in one window and not another — and the schema is resolved before execution
// anyway (§3.3), so the work happens here rather than being deferred to the first Next.
func (o *aggregateOverTime) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.schema, nil
}

// collect fills perStep with each step's window aggregates, preferring the one-call grid path.
func (o *aggregateOverTime) collect(
	ctx context.Context, refs []int64, rngMs int64, perStep [][]WindowAggregate,
) error {
	if o.grid != nil {
		if grid, ok := gridFor(refs, rngMs); ok {
			return o.collectGrid(ctx, grid, perStep)
		}
	}

	for i, maxt := range refs {
		// An @-modified selector pins every step to the same window, so one call answers them
		// all; without this the engine would ask storage the identical question once per step.
		if i > 0 && refs[i-1] == maxt {
			perStep[i] = perStep[i-1]
			continue
		}

		aggs, err := o.scanner.AggregateOverTime(ctx, maxt-rngMs, maxt, o.matchers)
		if err != nil {
			return errors.Wrapf(err, "aggregate over time at %d", maxt)
		}

		perStep[i] = aggs
	}

	return nil
}

// collectGrid pivots one grid call, which is series-major, into the step-major shape load needs.
// Windows a series had no sample in are dropped rather than carried as zero-count entries, so the
// resulting series set matches the per-step path's exactly.
func (o *aggregateOverTime) collectGrid(
	ctx context.Context, grid WindowGrid, perStep [][]WindowAggregate,
) error {
	series, err := aggregateGrid(ctx, o.grid, grid, o.matchers)
	if err != nil {
		return err
	}

	for i := range series {
		for step, w := range series[i].Windows {
			if w.Count == 0 {
				continue
			}

			perStep[step] = append(perStep[step], WindowAggregate{
				Labels:    series[i].Labels,
				Aggregate: w,
			})
		}
	}

	return nil
}

func (o *aggregateOverTime) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	var (
		refs   = refTimes(o.ec.Steps, o.offset, o.at)
		steps  = len(refs)
		rngMs  = o.rng.Milliseconds()
		series []labels.Labels
		index  = map[uint64][]SeriesRef{}
		// Per-step aggregates, collected before the value grid can be sized.
		perStep = make([][]WindowAggregate, steps)
	)

	if err := o.collect(ctx, refs, rngMs, perStep); err != nil {
		return err
	}

	for i := range perStep {
		aggs := perStep[i]
		for j := range aggs {
			// Range-vector functions drop __name__; none of the pushable folds retain it.
			ls := dropMetricName(aggs[j].Labels)
			aggs[j].Labels = ls

			if _, ok := lookupRefIn(series, index, ls); !ok {
				ref := SeriesRef(len(series))
				series = append(series, ls)
				h := ls.Hash()
				index[h] = append(index[h], ref)
			}
		}
	}

	o.schema = NewSchema(series)
	o.byHash = indexByHash(o.schema)

	o.values = make([]float64, len(series)*steps)
	o.valid = make([]bool, len(series)*steps)

	for step, aggs := range perStep {
		for j := range aggs {
			a := &aggs[j]
			if a.Count == 0 {
				continue // No sample in the window: PromQL emits nothing at this step.
			}

			ref, ok := lookupRef(o.schema, o.byHash, a.Labels)
			if !ok {
				return errors.Errorf("series %s absent from resolved schema", a.Labels)
			}

			v := o.fold(*a)
			if math.IsNaN(v) && o.fnName != "sum_over_time" && o.fnName != "avg_over_time" {
				// A NaN from a reducer that cannot legitimately produce one means the window was
				// empty in a way Count did not report; skip rather than emit a fabricated NaN.
				continue
			}

			i := int(ref)*steps + step
			o.values[i] = v
			o.valid[i] = true
		}
	}

	return nil
}

func (o *aggregateOverTime) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	steps := o.ec.NumSteps()

	for int(o.next) < o.schema.Len() {
		ref := o.next
		o.next++

		o.out.Resize(ref, steps)

		row := int(ref) * steps
		for step := range steps {
			if o.valid[row+step] {
				o.out.Set(step, o.values[row+step])
			}
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}

	return nil, nil
}

// lookupRefIn resolves ls against a schema still under construction. It exists because the series
// set is only known once every window has been folded, so [lookupRef]'s resolved [Schema] is not
// available yet.
func lookupRefIn(series []labels.Labels, byHash map[uint64][]SeriesRef, ls labels.Labels) (SeriesRef, bool) {
	for _, ref := range byHash[ls.Hash()] {
		if labels.Equal(series[ref], ls) {
			return ref, true
		}
	}

	return 0, false
}
