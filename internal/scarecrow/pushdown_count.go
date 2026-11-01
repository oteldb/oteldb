package scarecrow

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"

	"github.com/oteldb/oteldb/internal/xspan"
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

	// Pinned timestamps reuse a window; report storage calls rather than output steps.
	ctx, span := o.ec.span(ctx, "scarecrow.CountSeries.PerWindow",
		attribute.Int("promql.calls", countWindowCalls(refs, o.at)),
	)
	defer span.End()

	var n uint64
	for step, maxt := range refs {
		if step == 0 || maxt != refs[step-1] {
			var err error
			n, err = o.counter.CountSeries(ctx, maxt-lookback, maxt, o.matchers)
			if err != nil {
				xspan.Fail(span, err)
				return errors.Wrapf(err, "count series at %d", maxt)
			}
		}

		// An empty selector yields no series at all, not a zero — `count(absent_metric)` is empty.
		if n > 0 {
			o.out.Set(step, float64(n))

			if err := o.ec.charge(1); err != nil {
				return err
			}
		}
	}

	return nil
}

// countGrid answers every step from one grid call: a series counts at a step when it has any
// sample in that step's window, which is exactly a non-zero aggregate count.
func (o *countSeries) countGrid(ctx context.Context, grid WindowGrid) error {
	if counter, ok := o.grid.(GridSeriesCounter); ok {
		counts, err := counter.CountSeriesGrid(ctx, grid, "", o.matchers)
		if !errors.Is(err, ErrCountGridUnsupported) {
			if err != nil {
				return err
			}
			values := counts[""]
			if len(counts) > 1 || len(counts) == 1 && len(values) != grid.NumSteps {
				return errors.New("count grid returned invalid ungrouped counts")
			}
			if err := o.ec.charge(grid.NumSteps); err != nil {
				return err
			}
			for step, n := range values {
				if n != 0 {
					o.out.Set(step, float64(n))
				}
			}
			return nil
		}
	}
	series, err := aggregateGrid(ctx, o.ec, o.grid, grid, o.matchers)
	if err != nil {
		return err
	}

	counts := make([]float64, grid.NumSteps)
	for i := range series {
		for step, w := range series[i].Windows {
			if w.Rows > 0 {
				counts[step]++
			}
		}
	}

	// `count` without grouping keeps one number per step, whatever the folded cardinality was.
	if err := o.ec.charge(grid.NumSteps); err != nil {
		return err
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

// collect reads single-window counts, reusing a pinned timestamp across steps.
func (o *countSeriesBy) collect(ctx context.Context, refs []int64) ([]map[string]uint64, error) {
	lookback := o.ec.LookbackDelta.Milliseconds()

	// See the note in [countSeries.count]: the call count is the whole diagnosis.
	ctx, span := o.ec.span(ctx, "scarecrow.CountSeriesBy.PerWindow",
		attribute.Int("promql.calls", countWindowCalls(refs, o.at)),
		attribute.String("promql.by", o.by),
	)
	defer span.End()

	perStep := make([]map[string]uint64, len(refs))

	var counts map[string]uint64
	for i, maxt := range refs {
		if i == 0 || maxt != refs[i-1] {
			var err error
			counts, err = o.counter.CountSeriesBy(ctx, maxt-lookback, maxt, o.by, o.matchers)
			if err != nil {
				xspan.Fail(span, err)
				return nil, errors.Wrapf(err, "count series by %s at %d", o.by, maxt)
			}
		}

		if err := o.ec.charge(len(counts)); err != nil {
			return nil, err
		}

		perStep[i] = counts
	}

	return perStep, nil
}

func (o *countSeriesBy) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	refs := refTimes(o.ec.Steps, o.offset, o.at)
	if o.grid != nil {
		if grid, ok := gridFor(refs, o.ec.LookbackDelta.Milliseconds()); ok {
			return o.loadGrid(ctx, grid)
		}
	}
	var (
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

func countWindowCalls(refs []int64, at *int64) int {
	if at != nil && len(refs) > 0 {
		return 1
	}
	return len(refs)
}
