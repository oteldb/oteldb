package scarecrow

import (
	"sync/atomic"

	"github.com/prometheus/prometheus/promql"
)

// sampleBudget caps how many samples one query may load, mirroring the upstream engine's
// MaxSamples so a runaway query fails with an error instead of exhausting the process.
//
// It counts samples as they are read from the [Scanner] and as raw matrices are materialized —
// the two places data enters the engine. It is deliberately cumulative over the whole query
// rather than a live high-water mark: the columnar model holds one series' raw samples at a
// time (see [Samples]), so a peak gauge would never trip on the very shape that hurts, a scan
// touching millions of series. Counting every sample read bounds the work a single query can
// demand of storage, which is what the limit is for.
//
// One budget is shared by every chunk of a range query, so time-chunking cannot be used to
// evade it, and it is safe for concurrent use because [concurrent] evaluates two subtrees at
// once.
//
// It bounds what the *engine* materializes, which is not the same as what storage reads. A
// pushdown is charged for the folded values it returns (one per series per step), not for the
// samples storage scanned to fold them — `count(x)` over a million series charges one value per
// step, because that is all the engine ever holds. Bounding the scan itself has to happen in
// storage, where the counts are known; see oteldb/storage#263.
type sampleBudget struct {
	max  int64
	used atomic.Int64
}

// newSampleBudget returns a budget capping a query at limit samples. A limit <= 0 disables it.
func newSampleBudget(limit int) *sampleBudget {
	return &sampleBudget{max: int64(limit)}
}

// add charges n samples against the budget, returning [promql.ErrTooManySamples] once the query
// has read more than it is allowed. A nil or disabled budget always admits.
func (b *sampleBudget) add(n int) error {
	if b == nil || b.max <= 0 || n <= 0 {
		return nil
	}

	if b.used.Add(int64(n)) > b.max {
		return promql.ErrTooManySamples("query execution")
	}

	return nil
}

// Used reports how many samples have been charged so far. It is used for the query's trace
// attributes and by tests.
func (b *sampleBudget) Used() int64 {
	if b == nil {
		return 0
	}

	return b.used.Load()
}
