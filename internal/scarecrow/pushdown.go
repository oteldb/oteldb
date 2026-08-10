package scarecrow

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
)

// A pushdown is an optional [Scanner] capability: a computation the engine would otherwise do
// itself, which storage can answer more cheaply because it holds the data in a form the engine
// never sees. `internal/storagebackend` reaches these through the Thanos fork today by
// intercepting scanner construction and pattern-matching the logical node (docs §2.2); here they
// are plain planner rules over a capability the scanner either has or does not.
//
// Every pushdown is an optimization and never a semantic change: a scanner that implements none
// of these answers every query identically, only slower. That property is what the tests assert,
// by running the same corpus with and without the capabilities present.
//
// Two obligations bind any implementer, and they are why these are opt-in rather than assumed:
//
//   - **Exact windows.** A window is PromQL's half-open `(mint, maxt]`, not storage's inclusive
//     range. An implementation that widens it produces wrong answers at window edges, which the
//     corpus will not always catch.
//   - **No staleness markers.** The engine drops Prometheus staleness markers during the fold
//     ([matrixFold.buildWindow]); a storage-side aggregate cannot, so it would count a stale
//     series as present. Only a scanner over data that never carries markers may implement
//     these — which is true of `oteldb/storage`, and is not true of the [NewQueryableScanner]
//     adapter over an arbitrary Prometheus store.
//
// Implementations must also apply the *full* matcher set. The storage facade pushes only the
// index-safe subset and re-checks the rest; that re-check belongs behind this interface, so the
// engine can treat what it gets back as final.

// Aggregate is a fold of one series' samples over one window. A zero Count means the series had
// no sample in that window, which PromQL renders as a gap rather than a zero.
type Aggregate struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// WindowAggregate is one series' aggregate over a single window.
type WindowAggregate struct {
	Labels labels.Labels

	Aggregate
}

// WindowGrid is an evaluation grid of NumSteps equally spaced windows: window i covers
// (Start + i*Step - Width, Start + i*Step].
//
// It is the shape a range query actually asks about, and asking for it in one call rather than
// one call per step is the difference between a pushdown that helps and one that is far slower
// than no pushdown at all — see [GridAggregateScanner].
type WindowGrid struct {
	// Start is the end timestamp of the first window, in unix milliseconds.
	Start int64
	// Step is the spacing between consecutive window ends, in milliseconds. Always > 0.
	Step int64
	// NumSteps is the number of windows. Always > 0.
	NumSteps int
	// Width is each window's width in milliseconds: the range for a range-vector function, the
	// lookback delta for a plain selector.
	Width int64
}

// GridAggregate is one series' aggregates across every window of a [WindowGrid].
type GridAggregate struct {
	Labels labels.Labels
	// Windows is index-aligned with the grid's steps and has exactly NumSteps entries. A window
	// the series had no sample in carries a zero Count.
	Windows []Aggregate
}

// GridAggregateScanner answers a whole [WindowGrid] in one call.
//
// This exists because the per-window interfaces below are a trap at range-query scale. They read
// as cheap — an index lookup, no samples decoded — but the engine has to call them once per step,
// and each call pays a fresh querier plus whatever fixed setup the storage side does. Measured
// against a live deployment, `count by (cpu)` over a 1h/15s grid (241 steps) took 11.9s through
// the per-step [GroupedSeriesCounter] path against 0.04s for the same query with no pushdown at
// all: ~49ms of fixed cost per step, ~240x slower than simply fetching the window once and
// counting in the engine.
//
// So a pushdown that is not grid-aware is not an optimization. Storage folds each series' samples
// once and slides them into every overlapping window, making the cost proportional to the data in
// range rather than to range/step times it.
//
// A scanner implementing this supersedes [AggregateScanner], [SeriesCounter] and
// [GroupedSeriesCounter] for range queries; those remain for instant queries (one step, where the
// per-window call is exactly right) and for scanners that cannot answer a grid.
type GridAggregateScanner interface {
	Scanner

	// AggregateGrid returns one entry per matching series with a sample in any window of the
	// grid, each carrying that series' aggregate in every window.
	AggregateGrid(ctx context.Context, grid WindowGrid, matchers []*labels.Matcher) ([]GridAggregate, error)
}

// AggregateScanner is a [Scanner] that can fold a range-vector window itself, answering the
// reducer family of `*_over_time` without shipping raw samples to the engine.
//
// This is the pushdown that matters most: the raw path materializes every sample in every step's
// window, which is [oteldb#1117](https://github.com/oteldb/oteldb/issues/1117).
type AggregateScanner interface {
	Scanner

	// AggregateOverTime returns one aggregate per matching series over the window (mint, maxt],
	// omitting series with no sample in it.
	AggregateOverTime(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]WindowAggregate, error)
}

// SeriesCounter is a [Scanner] that can answer `count(selector)` from its index, without reading
// a single sample.
type SeriesCounter interface {
	Scanner

	// CountSeries returns the number of matching series with at least one sample in (mint, maxt].
	CountSeries(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (uint64, error)
}

// GroupedSeriesCounter is the `count by (label)` analog of [SeriesCounter].
type GroupedSeriesCounter interface {
	Scanner

	// CountSeriesBy returns, per value of label, the number of matching series with at least one
	// sample in (mint, maxt]. Series without the label are counted under the empty string, which
	// is what PromQL grouping does with a missing label.
	CountSeriesBy(
		ctx context.Context, mint, maxt int64, label string, matchers []*labels.Matcher,
	) (map[string]uint64, error)
}

// overTimeFolds maps the `*_over_time` functions an [AggregateScanner] can answer to the fold
// from its aggregate to the function's value.
//
// The rest of the family is deliberately absent rather than approximated. `rate`/`increase` need
// counter-reset detection across the raw samples; `quantile_over_time` needs every value;
// `last_over_time`/`first_over_time` need a sample's position, not a reduction;
// `stddev`/`stdvar_over_time` need a second moment this aggregate does not carry. Each falls back
// to [matrixFold], which is correct, just slower.
var overTimeFolds = map[string]func(WindowAggregate) float64{
	"count_over_time":   func(a WindowAggregate) float64 { return float64(a.Count) },
	"sum_over_time":     func(a WindowAggregate) float64 { return a.Sum },
	"min_over_time":     func(a WindowAggregate) float64 { return a.Min },
	"max_over_time":     func(a WindowAggregate) float64 { return a.Max },
	"avg_over_time":     func(a WindowAggregate) float64 { return a.Sum / float64(a.Count) },
	"present_over_time": func(WindowAggregate) float64 { return 1 },
}
