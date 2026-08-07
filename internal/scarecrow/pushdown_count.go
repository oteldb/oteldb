package scarecrow

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
)

// countSeries answers `count(selector)` from a [SeriesCounter] without reading a sample.
//
// The window is the lookback window, because that is exactly what `count` means at a step: the
// number of series with a sample in (t-lookback, t]. Storage answers that from its index, so a
// query whose whole purpose is cardinality never touches sample data.
type countSeries struct {
	counter  SeriesCounter
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
	counter SeriesCounter, matchers []*labels.Matcher, label string,
	offset time.Duration, at *int64, ec *EvalContext,
) *countSeries {
	return &countSeries{
		counter:  counter,
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

	for step, maxt := range refTimes(o.ec.Steps, o.offset, o.at) {
		n, err := o.counter.CountSeries(ctx, maxt-o.ec.LookbackDelta.Milliseconds(), maxt, o.matchers)
		if err != nil {
			return nil, errors.Wrapf(err, "count series at %d", maxt)
		}

		// An empty selector yields no series at all, not a zero — `count(absent_metric)` is empty.
		if n > 0 {
			o.out.Set(step, float64(n))
		}
	}

	if o.out.Empty() {
		return nil, nil
	}

	return &o.out, nil
}

// countSeriesBy answers `count by (label) (selector)` from a [GroupedSeriesCounter]. It is the
// grouped analog of [countSeries] and carries the same window semantics.
type countSeriesBy struct {
	counter  GroupedSeriesCounter
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
	counter GroupedSeriesCounter, matchers []*labels.Matcher, label, by string,
	offset time.Duration, at *int64, ec *EvalContext,
) *countSeriesBy {
	return &countSeriesBy{
		counter:  counter,
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

// load counts every step, then pivots the per-step maps into the (ref, step) grid the engine
// emits from. Unlike [aggregateOverTime] this is bounded by the *group* count, not the series
// count, which is the whole point of pushing the grouping down.
func (o *countSeriesBy) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	var (
		refs    = refTimes(o.ec.Steps, o.offset, o.at)
		steps   = len(refs)
		perStep = make([]map[string]uint64, steps)
		values  []string
		seen    = map[string]bool{}
	)

	for i, maxt := range refs {
		counts, err := o.counter.CountSeriesBy(
			ctx, maxt-o.ec.LookbackDelta.Milliseconds(), maxt, o.by, o.matchers,
		)
		if err != nil {
			return errors.Wrapf(err, "count series by %s at %d", o.by, maxt)
		}

		perStep[i] = counts

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
