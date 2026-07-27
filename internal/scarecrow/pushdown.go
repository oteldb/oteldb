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

// WindowAggregate is one series' aggregate over a single window.
type WindowAggregate struct {
	Labels labels.Labels
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
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
