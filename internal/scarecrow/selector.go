package scarecrow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"go.opentelemetry.io/otel/attribute"
)

// vectorSelect folds a series' raw samples onto the step grid using PromQL lookback: step t
// takes the most recent sample in the half-open window (t-lookback, t].
//
// It is one of the two leaves that consume [Samples]. It holds exactly one series' raw samples
// at a time — fold on arrival, then move on — so the raw level is O(1) in series regardless of
// query cardinality.
type vectorSelect struct {
	scanner  Scanner
	matchers []*labels.Matcher
	offset   time.Duration
	// at pins the evaluation timestamp (the @ modifier) in unix milliseconds. When set, every
	// step resolves against it, which is what makes such a selector step-invariant.
	at *int64
	// emitTimestamp makes the selector report each chosen sample's own timestamp in seconds
	// instead of its value. It backs timestamp(<selector>), whose result is a property of the
	// sample rather than of the step — the same device upstream calls SelectTimestamp.
	emitTimestamp bool
	ec            *EvalContext

	schema *Schema
	byHash map[uint64][]SeriesRef

	iter SeriesIterator
	out  Column
}

func newVectorSelect(
	sc Scanner, matchers []*labels.Matcher, offset time.Duration, at *int64, ec *EvalContext,
) *vectorSelect {
	return &vectorSelect{scanner: sc, matchers: matchers, offset: offset, at: at, ec: ec}
}

// newTimestampSelect returns a selector reporting sample timestamps, for timestamp(<selector>).
func newTimestampSelect(
	sc Scanner, matchers []*labels.Matcher, offset time.Duration, at *int64, ec *EvalContext,
) *vectorSelect {
	o := newVectorSelect(sc, matchers, offset, at, ec)
	o.emitTimestamp = true

	return o
}

// refTimes returns each step's evaluation timestamp: the step itself shifted by the offset, or
// the pinned @ timestamp repeated when one is set.
func refTimes(steps []int64, offset time.Duration, at *int64) []int64 {
	off := offset.Milliseconds()

	out := make([]int64, len(steps))
	for i, s := range steps {
		if at != nil {
			out[i] = *at - off
			continue
		}

		out[i] = s - off
	}

	return out
}

func (o *vectorSelect) String() string {
	return fmt.Sprintf("VectorSelect(%s)", matchersString(o.matchers))
}

func (o *vectorSelect) Children() []Operator { return nil }

func (o *vectorSelect) Close() error {
	if o.iter == nil {
		return nil
	}

	err := o.iter.Close()
	o.iter = nil

	return err
}

// window returns the fetch bounds covering every step's lookback window.
func (o *vectorSelect) window() (mint, maxt int64) {
	refs := refTimes(o.ec.Steps, o.offset, o.at)

	return refs[0] - o.ec.LookbackDelta.Milliseconds(), refs[len(refs)-1]
}

func (o *vectorSelect) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	// An empty step grid (a degenerate subquery, e.g. step > range missing every aligned tick
	// in the window) trivially selects nothing; window() assumes at least one step.
	if len(o.ec.Steps) == 0 {
		o.schema = NewSchema(nil)

		return o.schema, nil
	}

	mint, maxt := o.window()

	ctx, span := o.ec.span(ctx, "scarecrow.Series",
		attribute.String("promql.selector", matchersString(o.matchers)),
		attribute.Int64("promql.mint", mint),
		attribute.Int64("promql.maxt", maxt),
	)
	defer span.End()

	series, err := o.scanner.Series(ctx, mint, maxt, o.matchers)
	if err != nil {
		span.RecordError(err)

		return nil, errors.Wrap(err, "enumerate series")
	}

	span.SetAttributes(attribute.Int("promql.series", len(series)))

	// timestamp() yields a property of the sample, not the metric, so __name__ is dropped.
	if o.emitTimestamp {
		for i, ls := range series {
			series[i] = dropMetricName(ls)
		}
	}

	o.schema = NewSchema(series)
	o.byHash = indexByHash(o.schema)

	return o.schema, nil
}

func (o *vectorSelect) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	schema, err := o.Schema(ctx)
	if err != nil {
		return nil, err
	}

	if len(o.ec.Steps) == 0 || schema.Len() == 0 {
		return nil, nil
	}

	if o.iter == nil {
		mint, maxt := o.window()

		scanCtx, span := o.ec.span(ctx, "scarecrow.Scan",
			attribute.String("promql.selector", matchersString(o.matchers)),
			attribute.Int64("promql.mint", mint),
			attribute.Int64("promql.maxt", maxt),
		)

		it, err := o.scanner.Scan(scanCtx, mint, maxt, o.matchers)

		// The span covers opening the scan, not draining it: the iterator outlives this call and
		// is consumed lazily across many Next calls, so ending it here is the honest boundary.
		span.End()

		if err != nil {
			span.RecordError(err)

			return nil, errors.Wrap(err, "scan series")
		}
		o.iter = it
	}

	for {
		s, err := o.iter.Next(ctx)
		if err != nil {
			return nil, err
		}

		if s == nil {
			return nil, nil
		}

		ls := s.Labels
		if o.emitTimestamp {
			ls = dropMetricName(ls)
		}

		ref, ok := lookupRef(o.schema, o.byHash, ls)
		if !ok {
			// The scan returned a series the plan-time enumeration did not. Skipping would
			// silently drop data, so surface it: the two calls must see the same window.
			return nil, errors.Errorf("series %s absent from resolved schema", ls)
		}

		o.out.Resize(ref, o.ec.NumSteps())
		o.foldLookback(s)

		// PromQL omits a series with no sample at any step.
		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}
}

// foldLookback walks the step grid and the sample list together. Both are ascending, so one
// forward pass suffices — no per-step binary search.
func (o *vectorSelect) foldLookback(s *Samples) {
	var (
		lookback = o.ec.LookbackDelta.Milliseconds()
		refs     = refTimes(o.ec.Steps, o.offset, o.at)
		// idx is the count of samples with T <= refTime, so idx-1 is the candidate.
		idx int
	)

	for k, refTime := range refs {
		for idx < len(s.T) && s.T[idx] <= refTime {
			idx++
		}

		if idx == 0 {
			continue
		}

		i := idx - 1
		// The lookback window is left-open: a sample exactly at refTime-lookback is too old.
		if s.T[i] <= refTime-lookback {
			continue
		}

		// A staleness marker makes the series absent from here until the next real sample.
		if value.IsStaleNaN(s.V[i]) {
			continue
		}

		if o.emitTimestamp {
			o.out.Set(k, float64(s.T[i])/1000)
			continue
		}

		o.out.Set(k, s.V[i])
	}
}

// indexByHash groups a schema's refs by label hash, for resolving a scanned series back to its
// plan-time ref.
func indexByHash(s *Schema) map[uint64][]SeriesRef {
	m := make(map[uint64][]SeriesRef, s.Len())
	for i, h := range s.Hashes {
		m[h] = append(m[h], SeriesRef(i))
	}

	return m
}

// lookupRef resolves a scanned series' labels to its schema ref, comparing labels rather than
// trusting the hash so a collision cannot mix two series' samples.
func lookupRef(s *Schema, byHash map[uint64][]SeriesRef, ls labels.Labels) (SeriesRef, bool) {
	for _, ref := range byHash[ls.Hash()] {
		if labels.Equal(s.At(ref), ls) {
			return ref, true
		}
	}

	return 0, false
}

func matchersString(ms []*labels.Matcher) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}

	return "{" + strings.Join(parts, ", ") + "}"
}
