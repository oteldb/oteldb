package chstorage

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/maypok86/otter"
	"go.opentelemetry.io/otel/metric"
)

// MetricsCacheEntry is per-series cached sample data.
type MetricsCacheEntry struct {
	mu         sync.RWMutex
	timestamps []int64   // sorted, milliseconds
	values     []float64 // parallel
	minTS      int64
	maxTS      int64 // watermark
}

func newMetricsCacheEntry() *MetricsCacheEntry {
	return &MetricsCacheEntry{
		minTS: math.MinInt64,
		maxTS: math.MinInt64,
	}
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

	// We return slices of internal arrays.
	// It is safe because we only append to them and never modify existing elements.
	tss = e.timestamps[start:end]
	vals = e.values[start:end]
	return tss, vals
}

// Append adds new points after current maxTS; returns new cost in bytes.
//
// Input MUST be sorted by timestamp.
func (e *MetricsCacheEntry) Append(ts []int64, vals []float64, untilMs int64) uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, t := range ts {
		// Skip points that are already cached or older than current watermark.
		// We use <= to avoid duplicates if ClickHouse returns the boundary point again.
		if t <= e.maxTS {
			continue
		}
		if t > untilMs {
			break
		}
		e.timestamps = append(e.timestamps, t)
		e.values = append(e.values, vals[i])
		if e.minTS == math.MinInt64 || t < e.minTS {
			e.minTS = t
		}
		if t > e.maxTS {
			e.maxTS = t
		}
	}

	// 16 bytes per sample (int64 ts + float64 val) + 128 bytes for struct/bookkeeping.
	return uint32(len(e.timestamps)*16 + 128)
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
	// Meter is OpenTelemetry meter to use for cache metrics.
	Meter metric.Meter
}

func (opts *MetricsCacheOptions) setDefaults() {
	if opts.SafetyLag <= 0 {
		opts.SafetyLag = time.Minute
	}
}

func (opts MetricsCacheOptions) validate() error {
	if opts.MaxBytes < 0 {
		return errors.New("max_bytes must be non-negative")
	}
	if opts.SafetyLag < 0 {
		return errors.New("safety_lag must be non-negative")
	}
	return nil
}

func newMetricsCache(opts MetricsCacheOptions) (*MetricsCache, error) {
	opts.setDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// otter.MustBuilder capacity is the maximum number of items if Cost is not set,
	// or the maximum total cost if Cost is set.
	builder := otter.MustBuilder[[16]byte, *MetricsCacheEntry](int(opts.MaxBytes)).
		Cost(func(_ [16]byte, e *MetricsCacheEntry) uint32 {
			e.mu.RLock()
			defer e.mu.RUnlock()
			return uint32(len(e.timestamps)*16 + 128)
		}).
		WithTTL(30 * time.Minute)

	if opts.Meter != nil {
		builder = builder.CollectStats()
	}

	cache, err := builder.Build()
	if err != nil {
		return nil, err
	}

	if opts.Meter != nil {
		if err := registerMetrics(opts.Meter, cache); err != nil {
			return nil, errors.Wrap(err, "register metrics")
		}
	}

	return &MetricsCache{
		cache:     cache,
		safetyLag: opts.SafetyLag,
	}, nil
}

func registerMetrics(meter metric.Meter, cache otter.Cache[[16]byte, *MetricsCacheEntry]) error {
	hits, err := meter.Int64ObservableCounter("chstorage.metrics_cache.hits")
	if err != nil {
		return err
	}
	misses, err := meter.Int64ObservableCounter("chstorage.metrics_cache.misses")
	if err != nil {
		return err
	}
	size, err := meter.Int64ObservableGauge("chstorage.metrics_cache.size")
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		stats := cache.Stats()
		observer.ObserveInt64(hits, stats.Hits())
		observer.ObserveInt64(misses, stats.Misses())
		observer.ObserveInt64(size, int64(cache.Size()))
		return nil
	}, hits, misses, size)
	return err
}
