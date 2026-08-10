package scarecrow

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
)

// countSeries answers `count(selector)` from a [SeriesCounter] without reading a sample.
//
// The window is the lookback window, because that is exactly what `count` means at a step: the
// number of series with a sample in (t-lookback, t]. Storage answers that from its index, so a
// query whose whole purpose is cardinality never touches sample data.
type countSeries struct {
	counter SeriesCounter
	// grid, when non-nil, answers every step in one call instead of one per step. See
	// [GridAggregateScanner] for why that distinction decides whether this pushdown helps at all.
	grid     GridAggregateScanner
	matchers []*labels.Matcher
	label    string

	offset time.Duration
	at     *int64
	ec     *EvalContext

	schema *Schema
	out    Column
	done   bool
}

func newCountSeries(
	counter SeriesCounter, grid GridAggregateScanner, matchers []*labels.Matcher, label string,
	offset time.Duration, at *int64, ec *EvalContext,
) *countSeries {
	return &countSeries{
		counter:  counter,
		grid:     grid,
		matchers: matchers,
		label:    label,
		offset:   offset,
		at:       at,
		ec:       ec,
	}
}

func (o *countSeries) String() string { return fmt.Sprintf("CountSeries(%s)", o.label) }

func (o *countSeries) Children() []Operator { return nil }

func (o *countSeries) Close() error { return nil }

// Schema is known without touching storage: `count` without grouping is one anonymous series.
func (o *countSeries) Schema(context.Context) (*Schema, error) {
	if o.schema == nil {
		o.schema = NewSchema([]labels.Labels{labels.EmptyLabels()})
	}

	return o.schema, nil
}

func (o *countSeries) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	o.out.Resize(0, o.ec.NumSteps())

	refs := refTimes(o.ec.Steps, o.offset, o.at)
	lookback := o.ec.LookbackDelta.Milliseconds()

	if err := o.count(ctx, refs, lookback); err != nil {
		return nil, err
	}

	if o.out.Empty() {
		return nil, nil
	}

	return &o.out, nil
}

// count fills o.out with the per-step series count, preferring the one-call grid path.
func (o *countSeries) count(ctx context.Context, refs []int64, lookback int64) error {
	if o.grid != nil {
		if grid, ok := gridFor(refs, lookback); ok {
			return o.countGrid(ctx, grid)
		}
	}

	// Per-window fallback: one storage call per step. Spanned as a group with the call count on
	// it, because the count is the diagnosis — a hundred sibling calls here is the pathology
	// [GridAggregateScanner] exists to remove, and it is invisible if only the total shows up.
	ctx, span := o.ec.span(ctx, "scarecrow.CountSeries.PerWindow",
		attribute.Int("promql.calls", len(refs)),
	)
	defer span.End()

	for step, maxt := range refs {
		n, err := o.counter.CountSeries(ctx, maxt-lookback, maxt, o.matchers)
		if err != nil {
			span.RecordError(err)

			return errors.Wrapf(err, "count series at %d", maxt)
		}

		// An empty selector yields no series at all, not a zero — `count(absent_metric)` is empty.
		if n > 0 {
			o.out.Set(step, float64(n))
		}
	}

	return nil
}

// countGrid answers every step from one grid call: a series counts at a step when it has any
// sample in that step's window, which is exactly a non-zero aggregate count.
func (o *countSeries) countGrid(ctx context.Context, grid WindowGrid) error {
	series, err := aggregateGrid(ctx, o.ec, o.grid, grid, o.matchers)
	if err != nil {
		return err
	}

	counts := make([]float64, grid.NumSteps)
	for i := range series {
		for step, w := range series[i].Windows {
			if w.Count > 0 {
				counts[step]++
			}
		}
	}

	for step, n := range counts {
		if n > 0 {
			o.out.Set(step, n)
		}
	}

	return nil
}

// countSeriesBy answers `count by (label) (selector)` from a [GroupedSeriesCounter]. It is the
// grouped analog of [countSeries] and carries the same window semantics.
type countSeriesBy struct {
	counter GroupedSeriesCounter
	// grid, when non-nil, answers every step in one call. See [countSeries.grid].
	grid     GridAggregateScanner
	matchers []*labels.Matcher
	label    string
	by       string

	offset time.Duration
	at     *int64
	ec     *EvalContext

	schema *Schema
	byHash map[uint64][]SeriesRef

	values []float64
	valid  []bool
	loaded bool

	next SeriesRef
	out  Column
}

func newCountSeriesBy(
	counter GroupedSeriesCounter, grid GridAggregateScanner, matchers []*labels.Matcher, label, by string,
	offset time.Duration, at *int64, ec *EvalContext,
) *countSeriesBy {
	return &countSeriesBy{
		counter:  counter,
		grid:     grid,
		matchers: matchers,
		label:    label,
		by:       by,
		offset:   offset,
		at:       at,
		ec:       ec,
	}
}

func (o *countSeriesBy) String() string {
	return fmt.Sprintf("CountSeriesBy(%s, by=%s)", o.label, o.by)
}

func (o *countSeriesBy) Children() []Operator { return nil }

func (o *countSeriesBy) Close() error { return nil }

func (o *countSeriesBy) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.schema, nil
}

// collect returns the per-value counts at each step, preferring the one-call grid path over one
// [GroupedSeriesCounter] call per step.
func (o *countSeriesBy) collect(ctx context.Context, refs []int64) ([]map[string]uint64, error) {
	lookback := o.ec.LookbackDelta.Milliseconds()

	if o.grid != nil {
		if grid, ok := gridFor(refs, lookback); ok {
			return o.collectGrid(ctx, grid)
		}
	}

	// See the note in [countSeries.count]: the call count is the whole diagnosis.
	ctx, span := o.ec.span(ctx, "scarecrow.CountSeriesBy.PerWindow",
		attribute.Int("promql.calls", len(refs)),
		attribute.String("promql.by", o.by),
	)
	defer span.End()

	perStep := make([]map[string]uint64, len(refs))

	for i, maxt := range refs {
		counts, err := o.counter.CountSeriesBy(ctx, maxt-lookback, maxt, o.by, o.matchers)
		if err != nil {
			span.RecordError(err)

			return nil, errors.Wrapf(err, "count series by %s at %d", o.by, maxt)
		}

		perStep[i] = counts
	}

	return perStep, nil
}

// collectGrid groups one grid call's series into per-step counts. The grouping happens here
// rather than in storage because it is trivially cheap once the series are in hand — the label
// value is on the series identity, no sample is consulted — and it keeps the pushdown seam a
// single general-purpose call rather than one variant per grouping shape.
func (o *countSeriesBy) collectGrid(ctx context.Context, grid WindowGrid) ([]map[string]uint64, error) {
	series, err := aggregateGrid(ctx, o.ec, o.grid, grid, o.matchers)
	if err != nil {
		return nil, err
	}

	perStep := make([]map[string]uint64, grid.NumSteps)
	for i := range perStep {
		perStep[i] = map[string]uint64{}
	}

	for i := range series {
		// A series without the label groups under "", matching PromQL's absent-label group.
		v := series[i].Labels.Get(o.by)

		for step, w := range series[i].Windows {
			if w.Count > 0 {
				perStep[step][v]++
			}
		}
	}

	return perStep, nil
}

// load counts every step, then pivots the per-step maps into the (ref, step) grid the engine
// emits from. Unlike [aggregateOverTime] this is bounded by the *group* count, not the series
// count, which is the whole point of pushing the grouping down.
func (o *countSeriesBy) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	var (
		refs   = refTimes(o.ec.Steps, o.offset, o.at)
		steps  = len(refs)
		values []string
		seen   = map[string]bool{}
	)

	perStep, err := o.collect(ctx, refs)
	if err != nil {
		return err
	}

	for _, counts := range perStep {
		for v, n := range counts {
			if n == 0 || seen[v] {
				continue
			}

			seen[v] = true
			values = append(values, v)
		}
	}

	// Sorted so the schema is deterministic regardless of map iteration order.
	slices.Sort(values)

	series := make([]labels.Labels, len(values))
	for i, v := range values {
		if v == "" {
			// PromQL grouping drops a label that no series in the group carries.
			series[i] = labels.EmptyLabels()
			continue
		}

		series[i] = labels.FromStrings(o.by, v)
	}

	o.schema = NewSchema(series)
	o.byHash = indexByHash(o.schema)
	o.values = make([]float64, len(series)*steps)
	o.valid = make([]bool, len(series)*steps)

	pos := make(map[string]int, len(values))
	for i, v := range values {
		pos[v] = i
	}

	for step, counts := range perStep {
		for v, n := range counts {
			if n == 0 {
				continue
			}

			i := pos[v]*steps + step
			o.values[i] = float64(n)
			o.valid[i] = true
		}
	}

	return nil
}

func (o *countSeriesBy) Next(ctx context.Context) (*Column, error) {
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
