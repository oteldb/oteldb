package storagebackend

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/extlabels"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

// aggregateOverTimeRangeOp answers a range-vector *_over_time query (the reducer family: count/sum/
// min/max/avg/present) from the aggregate-sidecar pushdown instead of the raw matrix selector. It is
// the range analog of [aggregateOverTimeOp]: where the matrix selector materializes every raw
// sample in every step's sliding window (buffering the whole window set live), this makes one
// [storage.Storage.AggregateMetricsWindowNamed] call for the whole grid — storage folds each
// series' samples once into step-wide buckets and slides them into every overlapping (maxt-range,
// maxt] window, so the cost is proportional to the data in range rather than to range/step times
// it — and streams the per-step result vectors, holding only O(result) rather than O(all raw
// samples in range).
//
// Each PromQL step t_i is evaluated over its exact sliding window (t_i - offset - range, t_i -
// offset], the same window the matrix selector uses, so the result is identical; the storage
// engine's evaluation grid is anchored at the query's own first step (not at a multiple of the
// step from the Unix epoch), which is what makes the returned windows land on the timestamps this
// operator actually asked for.
//
// Only pushed down for the sidecar-answerable folds and a plain selector (no projection, per-series
// filter, or @ modifier); rate/increase/quantile/… and anything else fall back to the matrix
// selector. See [scanners.NewMatrixSelector].
type aggregateOverTimeRangeOp struct {
	src      Source
	tenant   signal.TenantID
	matchers []*labels.Matcher
	fold     func(engine.SeriesAgg) float64
	fnName   string

	// Query grid, in milliseconds. Steps are start, start+step, …, ≤ end.
	start, end int64
	step       int64
	rangeMs    int64
	offset     int64
	numSteps   int // per-batch step cap (opts.NumStepsPerBatch()).

	loaded  bool
	loadErr error
	// series is the union of series appearing in any step, index-aligned with the SampleIDs emitted
	// in the step vectors.
	series []labels.Labels
	// stepTimes[i] is the i-th step's timestamp; stepVals[i] its (SampleID, value) pairs.
	stepTimes []int64
	stepVals  [][]sampleVal

	nextIdx int // emission cursor into stepTimes/stepVals.
}

// sampleVal is one series' folded value at a step, addressed by its index into series.
type sampleVal struct {
	id  uint64
	val float64
}

func newAggregateOverTimeRangeOp(
	src Source,
	tenant signal.TenantID,
	matchers []*labels.Matcher,
	fnName string,
	fold func(engine.SeriesAgg) float64,
	start, end, step, rangeMs, offset int64,
	numSteps int,
) *aggregateOverTimeRangeOp {
	return &aggregateOverTimeRangeOp{
		src:      src,
		tenant:   tenant,
		matchers: matchers,
		fold:     fold,
		fnName:   fnName,
		start:    start,
		end:      end,
		step:     step,
		rangeMs:  rangeMs,
		offset:   offset,
		numSteps: numSteps,
	}
}

func (o *aggregateOverTimeRangeOp) String() string {
	return fmt.Sprintf("[storagebackend.aggregateOverTimeRangeOp] %s({%v}[%s]) step=%s",
		o.fnName, o.matchers, time.Duration(o.rangeMs)*time.Millisecond, time.Duration(o.step)*time.Millisecond)
}

func (o *aggregateOverTimeRangeOp) Explain() []model.VectorOperator { return nil }

func (o *aggregateOverTimeRangeOp) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *aggregateOverTimeRangeOp) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if err := o.load(ctx); err != nil {
		return 0, err
	}

	if o.nextIdx >= len(o.stepTimes) || len(buf) == 0 {
		return 0, nil
	}

	maxSteps := len(buf)
	if o.numSteps > 0 && o.numSteps < maxSteps {
		maxSteps = o.numSteps
	}

	n := 0
	for n < maxSteps && o.nextIdx < len(o.stepTimes) {
		buf[n].Reset(o.stepTimes[o.nextIdx])
		vals := o.stepVals[o.nextIdx]
		for _, sv := range vals {
			buf[n].AppendSampleWithSizeHint(sv.id, sv.val, len(vals))
		}
		n++
		o.nextIdx++
	}

	return n, nil
}

// load folds every step's window once, building the union series set and the per-step values. It
// runs at most once; the [model.VectorOperator] contract calls Series before Next, and Next may be
// called concurrently only after Series, so a plain guard (not sync.Once) suffices as the engine
// drives a single operator sequentially.
func (o *aggregateOverTimeRangeOp) load(ctx context.Context) error {
	if o.loaded {
		return o.loadErr
	}
	o.loadErr = o.doLoad(ctx)
	o.loaded = true

	return o.loadErr
}

func (o *aggregateOverTimeRangeOp) doLoad(ctx context.Context) error {
	const nsPerMs = int64(time.Millisecond)

	// index maps a storage series identity to its stable SampleID (index into o.series). Keying by
	// content-addressed id — not the projected label set — keeps two series that project to the same
	// PromQL labels distinct, matching the matrix selector (one scanner per storage series).
	index := make(map[signal.SeriesID]uint64)
	var b labels.ScratchBuilder

	// One call for the whole grid: storage folds each series' samples once into step-wide buckets and
	// slides them into every (maxt-range, maxt] window, so the cost is proportional to the data in the
	// range rather than to the W/step overlap a per-step loop would pay. The evaluation grid is
	// anchored at the query's first step — PromQL's grid is not a multiple of the step.
	firstEval := o.start - o.offset

	aggs, err := o.src.AggregateMetricsWindowNamed(ctx, o.tenant, fetch.Request{
		Tenant:   o.tenant,
		Start:    (firstEval - o.rangeMs + 1) * nsPerMs, // lead-in: the first window opens a range early
		End:      (o.end - o.offset) * nsPerMs,
		Matchers: storagepromql.PushableMatchers(o.matchers),
	}, engine.WindowSpec{
		Step:   o.step * nsPerMs,
		Window: o.rangeMs * nsPerMs,
		Anchor: firstEval * nsPerMs,
	})
	if err != nil {
		return errors.Wrap(err, "aggregate metrics windows")
	}

	// Storage answers series-major (every window of a series together); the operator emits
	// step-major. Pivot through a per-eval-timestamp slot, so the walk is O(result), not O(steps ×
	// series).
	steps := 0
	if o.step > 0 && o.end >= o.start {
		steps = int((o.end-o.start)/o.step) + 1
	}

	o.stepTimes = make([]int64, 0, steps)
	o.stepVals = make([][]sampleVal, steps)

	for i := range steps {
		o.stepTimes = append(o.stepTimes, o.start+int64(i)*o.step)
	}

	for i := range aggs {
		na := &aggs[i]

		lset := storagepromql.PromLabels(na.Series)
		if !storagepromql.MatchesAll(lset, o.matchers) {
			continue
		}

		key := na.Series.Hash()

		id, ok := index[key]
		if !ok {
			id = uint64(len(o.series))
			index[key] = id

			// Range-vector functions drop the metric name (and the other schema metadata labels).
			dropped := extlabels.DropReserved(lset, b)
			b.Reset()
			o.series = append(o.series, dropped)
		}

		for _, w := range na.Windows {
			v := o.fold(w.SeriesAgg)
			if math.IsNaN(v) {
				continue
			}

			// The window's End is its evaluation timestamp, offset back into query time.
			evalT := w.End/nsPerMs + o.offset

			idx := int((evalT - o.start) / o.step)
			if o.step <= 0 || idx < 0 || idx >= steps {
				continue
			}

			o.stepVals[idx] = append(o.stepVals[idx], sampleVal{id: id, val: v})
		}
	}

	return nil
}
