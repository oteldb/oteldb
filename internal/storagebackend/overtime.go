package storagebackend

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/extlabels"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

// overTimeFold folds a per-series [engine.SeriesAgg] into the value of one PromQL *_over_time
// function. These are the functions the aggregate sidecar (count/sum/min/max) answers without
// decoding: avg is sum/count, present_over_time is "1 if any sample" (the facade omits empty
// series, so every returned series is present). rate/increase, quantile_over_time,
// last/first_over_time, stddev/stdvar_over_time, and friends need more than the sidecar and are
// not pushable — they fall back to the raw matrix selector.
var overTimeFold = map[string]func(engine.SeriesAgg) float64{
	"count_over_time":   func(a engine.SeriesAgg) float64 { return float64(a.Count) },
	"sum_over_time":     func(a engine.SeriesAgg) float64 { return a.Sum },
	"min_over_time":     func(a engine.SeriesAgg) float64 { return a.Min },
	"max_over_time":     func(a engine.SeriesAgg) float64 { return a.Max },
	"avg_over_time":     func(a engine.SeriesAgg) float64 { return a.Sum / float64(a.Count) },
	"present_over_time": func(engine.SeriesAgg) float64 { return 1 },
}

// aggregateOverTimeOp is a [model.VectorOperator] that answers an instant *_over_time query by
// folding [storage.Storage.AggregateMetricsNamed] (the stats-sidecar pushdown) instead of a raw
// fetch-and-fold. It is a drop-in replacement for the matrix selector plus its ringbuffer fold:
// its output is the function value per series, with the metric name dropped as PromQL does for
// range-vector functions.
//
// It is instant-only. A range query re-evaluates the function at each step over a sliding
// (t-range, t] window, which the storage library exposes only as step-aligned buckets (a different
// shape); the raw matrix selector stays correct there and is used instead. See
// docs/storage-integration.md.
type aggregateOverTimeOp struct {
	store    *storage.Storage
	tenant   signal.TenantID
	matchers []*labels.Matcher
	fold     func(engine.SeriesAgg) float64
	fnName   string

	evalT   int64 // milliseconds — the single evaluation timestamp
	rangeMs int64
	offset  int64

	once    sync.Once
	series  []labels.Labels // index-aligned with values; the StepVector SampleID is this index
	values  []float64
	loadErr error

	emitted bool
}

func newAggregateOverTimeOp(
	store *storage.Storage,
	tenant signal.TenantID,
	matchers []*labels.Matcher,
	fnName string,
	fold func(engine.SeriesAgg) float64,
	evalT, rangeMs, offsetMs int64,
) *aggregateOverTimeOp {
	return &aggregateOverTimeOp{
		store:    store,
		tenant:   tenant,
		matchers: matchers,
		fold:     fold,
		fnName:   fnName,
		evalT:    evalT,
		rangeMs:  rangeMs,
		offset:   offsetMs,
	}
}

func (o *aggregateOverTimeOp) String() string {
	return fmt.Sprintf("[storagebackend.aggregateOverTimeOp] %s({%v}[%s])", o.fnName, o.matchers, time.Duration(o.rangeMs)*time.Millisecond)
}

func (o *aggregateOverTimeOp) Explain() []model.VectorOperator { return nil }

func (o *aggregateOverTimeOp) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *aggregateOverTimeOp) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if o.emitted {
		return 0, nil
	}

	if err := o.load(ctx); err != nil {
		return 0, err
	}

	o.emitted = true
	if len(o.series) == 0 || len(buf) == 0 {
		return 0, nil
	}

	buf[0].Reset(o.evalT)
	hint := len(o.values)
	for i, v := range o.values {
		buf[0].AppendSampleWithSizeHint(uint64(i), v, hint)
	}

	return 1, nil
}

// load runs the pushdown once: it asks the storage facade for the labeled whole-range aggregate
// over the PromQL window, drops the metric name (as range-vector functions do), and re-checks the
// full matcher set (the facade, like the Queryable, pushes only the index-safe subset).
func (o *aggregateOverTimeOp) load(ctx context.Context) error {
	o.once.Do(func() { o.loadErr = o.doLoad(ctx) })

	return o.loadErr
}

func (o *aggregateOverTimeOp) doLoad(ctx context.Context) error {
	// PromQL evaluates the range vector at (evalT - offset] over a (range] window: the included
	// samples are (mint, maxt]. Storage's fetch window is inclusive [start, end] in nanoseconds,
	// so mint is exclusive (start = mint+1 ms) and maxt inclusive.
	maxt := o.evalT - o.offset
	mint := maxt - o.rangeMs

	const nsPerMs = int64(time.Millisecond)

	aggs, err := o.store.AggregateMetricsNamed(ctx, o.tenant, fetch.Request{
		Tenant:   o.tenant,
		Start:    (mint + 1) * nsPerMs, // PromQL's (mint, maxt] ⇒ storage's inclusive [mint+1, maxt].
		End:      maxt * nsPerMs,
		Matchers: storagepromql.PushableMatchers(o.matchers),
	})
	if err != nil {
		return errors.Wrap(err, "aggregate metrics")
	}

	o.series = make([]labels.Labels, 0, len(aggs))
	o.values = make([]float64, 0, len(aggs))

	var b labels.ScratchBuilder
	for i := range aggs {
		la := &aggs[i]

		lset := storagepromql.PromLabels(la.Series)
		if !storagepromql.MatchesAll(lset, o.matchers) {
			continue
		}

		// Range-vector functions drop the metric name (and the other schema metadata labels).
		lset = extlabels.DropReserved(lset, b)
		b.Reset()

		v := o.fold(la.SeriesAgg)
		if math.IsNaN(v) {
			continue
		}

		o.series = append(o.series, lset)
		o.values = append(o.values, v)
	}

	// Sort both slices in lockstep by label set, for deterministic output (Prometheus returns
	// vectors sorted by labels).
	order := make([]int, len(o.series))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, c int) bool {
		return labels.Compare(o.series[order[a]], o.series[order[c]]) < 0
	})
	sortedSeries := make([]labels.Labels, len(order))
	sortedVals := make([]float64, len(order))
	for i, idx := range order {
		sortedSeries[i] = o.series[idx]
		sortedVals[i] = o.values[idx]
	}
	o.series = sortedSeries
	o.values = sortedVals

	return nil
}
