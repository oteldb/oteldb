package metricscache

import (
	"math"
	"slices"
	"sync"
)

const (
	// PointCost is the estimated memory cost per cached point in bytes.
	// 8 (int64 delta) + 8 (float64) = 16.
	PointCost = 16
	// EntryOverhead is the fixed overhead per cache entry in bytes.
	EntryOverhead = 128
)

// EntryCost returns the estimated memory cost of an entry with the given number of points.
func EntryCost(points int) uint32 {
	return uint32(points)*PointCost + EntryOverhead
}

// Entry is per-series cached sample data.
type Entry struct {
	mu      sync.RWMutex
	deltaTS []int64   // ms deltas from minTS; replaces timestamps []int64
	values  []float64 // parallel to deltaTS
	minTS   int64
	maxTS   int64 // watermark
}

// NewEntry creates a new empty Entry.
func NewEntry() *Entry {
	return &Entry{
		minTS: math.MinInt64,
		maxTS: math.MinInt64,
	}
}

// Watermarks returns the coverage bounds [minTS, maxTS] of this entry.
func (e *Entry) Watermarks() (minTS, maxTS int64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.minTS, e.maxTS
}

// Len returns the number of cached data points.
func (e *Entry) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.deltaTS)
}

// Cost returns the estimated memory cost of the entry in bytes.
func (e *Entry) Cost() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return EntryCost(len(e.deltaTS))
}

// Slice returns a copy of samples in [fromMs, toMs].
func (e *Entry) Slice(fromMs, toMs int64) (tss []int64, vals []float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.minTS == math.MinInt64 {
		return nil, nil
	}

	fromDelta := fromMs - e.minTS
	toDelta := toMs - e.minTS

	start, _ := slices.BinarySearch(e.deltaTS, fromDelta)
	end, ok := slices.BinarySearch(e.deltaTS, toDelta)
	if ok {
		// Include toMs.
		end++
	}

	if start >= end {
		return nil, nil
	}

	tss = make([]int64, end-start)
	for i, d := range e.deltaTS[start:end] {
		tss[i] = e.minTS + d
	}
	vals = e.values[start:end]
	return tss, vals
}

// Append stores points into the entry.
//
// Points older than current minTS are prepended (backward-fill when the caller fetches
// a wider historical range than what was previously cached). Points already inside
// [minTS, maxTS] are skipped as duplicates. Points in (maxTS, untilMs] are appended.
//
// Input MUST be sorted by timestamp.
func (e *Entry) Append(ts []int64, vals []float64, untilMs int64) uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(ts) == 0 {
		return EntryCost(len(e.deltaTS))
	}

	if e.minTS == math.MinInt64 {
		// Cache is empty: append all points up to untilMs.
		for i, t := range ts {
			if t > untilMs {
				break
			}
			if e.minTS == math.MinInt64 {
				e.minTS = t
			}
			e.deltaTS = append(e.deltaTS, t-e.minTS)
			e.values = append(e.values, vals[i])
			if t > e.maxTS {
				e.maxTS = t
			}
		}
		return EntryCost(len(e.deltaTS))
	}

	// Cache already has data. Split ts into:
	//   prefix: points older than minTS (need prepend)
	//   suffix: points newer than maxTS, up to untilMs (need append)
	//   middle: already covered by [minTS, maxTS] (skip)
	splitIdx, _ := slices.BinarySearch(ts, e.minTS)

	// Prepend points older than minTS.
	if splitIdx > 0 {
		prTS := ts[:splitIdx]
		prVals := vals[:splitIdx]

		oldMinTS := e.minTS
		newMinTS := prTS[0]
		shift := oldMinTS - newMinTS
		for i := range e.deltaTS {
			e.deltaTS[i] += shift
		}
		e.minTS = newMinTS

		newDelta := make([]int64, len(prTS)+len(e.deltaTS))
		for i, t := range prTS {
			newDelta[i] = t - newMinTS
		}
		copy(newDelta[len(prTS):], e.deltaTS)
		e.deltaTS = newDelta

		e.values = slices.Concat(
			prVals,
			e.values,
		)
	}

	// Append points newer than maxTS, up to untilMs.
	for i, t := range ts[splitIdx:] {
		// Use <= to avoid duplicates if ClickHouse returns the boundary point again.
		if t <= e.maxTS {
			continue
		}
		if t > untilMs {
			break
		}
		e.deltaTS = append(e.deltaTS, t-e.minTS)
		e.values = append(e.values, vals[splitIdx+i])
		if t > e.maxTS {
			e.maxTS = t
		}
	}

	return EntryCost(len(e.deltaTS))
}

// MarkFetched advances the watermark to record that [fetchFrom, untilMs] has been
// confirmed queried — even when no data points exist in that range.
//
// This lets the cache treat a series with no data as a cache hit so that
// subsequent queries for the same range skip the ClickHouse round-trip.
func (e *Entry) MarkFetched(fetchFrom, untilMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.minTS == math.MinInt64 {
		e.minTS = fetchFrom
	} else if fetchFrom < e.minTS {
		shift := e.minTS - fetchFrom
		for i := range e.deltaTS {
			e.deltaTS[i] += shift
		}
		e.minTS = fetchFrom
	}
	if untilMs > e.maxTS {
		e.maxTS = untilMs
	}
}

// ToBlock serializes the entry to a compressed Block for disk storage.
func (e *Entry) ToBlock() (Block, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Decode deltaTS to full timestamps.
	ts := make([]int64, len(e.deltaTS))
	for i, d := range e.deltaTS {
		ts[i] = e.minTS + d
	}

	return EncodeBlock(ts, e.values, e.minTS, e.maxTS)
}

// FromBlock deserializes a Block back into an Entry.
func FromBlock(b Block) (*Entry, error) {
	ts, vals, err := b.Decode()
	if err != nil {
		return nil, err
	}

	e := NewEntry()
	if len(ts) > 0 {
		e.Append(ts, vals, b.MaxTS)
	}
	// Restore watermarks (handles empty-series case where minTS/maxTS were set by MarkFetched).
	e.MarkFetched(b.MinTS, b.MaxTS)
	return e, nil
}
