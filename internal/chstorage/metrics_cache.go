package chstorage

import (
	"slices"
	"sync"
	"time"

	"github.com/maypok86/otter"
)

// MetricsCacheEntry is per-series cached sample data.
type MetricsCacheEntry struct {
	mu         sync.RWMutex
	timestamps []int64   // sorted, milliseconds
	values     []float64 // parallel
	minTS      int64
	maxTS      int64 // watermark
}

// Slice returns a copy of samples in [fromMs, toMs].
func (e *MetricsCacheEntry) Slice(fromMs, toMs int64) (tss []int64, vals []float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	start, _ := slices.BinarySearch(e.timestamps, fromMs)
	end, ok := slices.BinarySearch(e.timestamps, toMs)
	if ok {
		// Include toMs.
		end++
	}

	if start >= end {
		return nil, nil
	}

	tss = slices.Clone(e.timestamps[start:end])
	vals = slices.Clone(e.values[start:end])
	return tss, vals
}

// Append adds new points after current maxTs; returns new cost in bytes.
func (e *MetricsCacheEntry) Append(ts []int64, vals []float64, untilMs int64) uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, t := range ts {
		if t <= e.maxTS {
			continue
		}
		if t > untilMs {
			break
		}
		e.timestamps = append(e.timestamps, t)
		e.values = append(e.values, vals[i])
		if e.minTS == 0 || t < e.minTS {
			e.minTS = t
		}
		if t > e.maxTS {
			e.maxTS = t
		}
	}

	return uint32(len(e.timestamps) * 16)
}

// MetricsCache wraps otter cache.
type MetricsCache struct {
	cache     otter.Cache[[16]byte, *MetricsCacheEntry]
	safetyLag time.Duration
}

// MetricsCacheOptions configures the cache.
type MetricsCacheOptions struct {
	// MaxBytes is max memory budget. Zero disables the cache.
	MaxBytes int64
	// SafetyLag is the duration from now that is not cached.
	SafetyLag time.Duration // default 60s
}

func (opts *MetricsCacheOptions) setDefaults() {
	if opts.SafetyLag <= 0 {
		opts.SafetyLag = time.Minute
	}
}

func newMetricsCache(opts MetricsCacheOptions) (*MetricsCache, error) {
	opts.setDefaults()

	// otter.MustBuilder capacity is the maximum number of items if Cost is not set,
	// or the maximum total cost if Cost is set.
	cache, err := otter.MustBuilder[[16]byte, *MetricsCacheEntry](int(opts.MaxBytes)).
		Cost(func(_ [16]byte, e *MetricsCacheEntry) uint32 {
			e.mu.RLock()
			defer e.mu.RUnlock()
			return uint32(len(e.timestamps) * 16)
		}).
		Build()
	if err != nil {
		return nil, err
	}

	return &MetricsCache{
		cache:     cache,
		safetyLag: opts.SafetyLag,
	}, nil
}
