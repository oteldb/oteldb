package scarecrow

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
)

// Samples is one series' raw samples over the fetch window, borrowed from the producing
// scanner. Timestamps are unix milliseconds, ascending.
//
// This is the ragged level. It never crosses an [Operator] boundary: only the two selector
// leaves consume it, folding it into a [Column] and releasing it immediately. Exactly one
// series' samples are borrowed per iterator or fold worker at a time, so raw residency is bounded
// by the worker count rather than query cardinality.
//
// The slices are borrowed from the scanner and are invalid after the next [SeriesIterator.Next]
// or after Close.
type Samples struct {
	Labels labels.Labels
	T      []int64
	V      []float64
	// Weights carries each sample's lossy-sampling scale factor, where a kept sample with
	// weight N stands for N original samples. It is nil when no sampling occurred, which is the
	// common case. Weights are consumed entirely inside the fold — no operator above a selector
	// ever sees one. See docs/promql-engine.md §3.5 for the per-function policy.
	Weights []float64
}

// Weight returns sample i's scale factor, defaulting to 1 when unsampled.
func (s *Samples) Weight(i int) float64 {
	if s.Weights == nil {
		return 1
	}

	return s.Weights[i]
}

// SeriesIterator yields one series' raw samples at a time. Next returns nil at end of stream.
// The returned Samples is owned by the iterator and valid only until the following call.
type SeriesIterator interface {
	Next(ctx context.Context) (*Samples, error)
	Close() error
}

// Scanner is the engine's storage seam: it resolves label matchers over a time window to a
// stream of per-series raw samples.
//
// It is deliberately narrower than storage.Queryable. The engine wants series-at-a-time
// columnar delivery, which is the shape oteldb/storage's fetch seam already produces; adapting
// a row-oriented Queryable to it (see [NewQueryableScanner]) costs one copy and is used for the
// upstream test corpus and as a fallback.
//
// **A Scanner must be safe for concurrent use.** One scanner serves a whole query, and a binary
// operator evaluates both of its subtrees at once ([concurrent]), so two selectors can be calling
// Series or Scan on it simultaneously. Each returned [SeriesIterator], by contrast, is owned by a
// single operator and is never shared.
type Scanner interface {
	// Series enumerates the matching series' label sets without materializing samples. The
	// engine calls this at plan time to freeze schemas before execution.
	Series(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]labels.Labels, error)

	// Scan streams raw samples for the matching series over [mint, maxt].
	Scan(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (SeriesIterator, error)

	Close() error
}

// PartitionScanner can distribute a single scan among independent consumers without rescanning
// or copying samples. It returns exactly workers iterators, all of which must be closed.
// Scanners without this capability retain serial evaluation.
type PartitionScanner interface {
	ScanPartitions(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, workers int) ([]PartitionIterator, error)
}

// QueryBudgetScanner installs one query-wide memory allowance before planning or execution.
// Implementations preserve an allowance already present on ctx.
type QueryBudgetScanner interface {
	WithQueryBudget(ctx context.Context) context.Context
}

// PartitionIterator borrows at most one series. Position identifies its most recent sample's
// zero-based position in the original unpartitioned stream; positions across partitions form
// one contiguous sequence and increase within each partition. Release ends the current borrow
// before the next Next, allowing consumers to wait without retaining raw storage buffers.
type PartitionIterator interface {
	SeriesIterator
	Position() int
	Release()
}
