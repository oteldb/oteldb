package chstorage

import (
	"cmp"
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/autometric"
	"github.com/maypok86/otter"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// MetricsCacheEntry is per-series cached sample data.
type MetricsCacheEntry struct {
	mu      sync.RWMutex
	deltaTS []int32   // ms deltas from minTS; replaces timestamps []int64
	values  []float64 // parallel
	minTS   int64
	maxTS   int64 // watermark
}

const (
	metricsCachePointCost     = 12
	metricsCacheEntryOverhead = 128
)

func metricsCacheCost(points int) uint32 {
	return uint32(points)*metricsCachePointCost + metricsCacheEntryOverhead
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

	if e.minTS == math.MinInt64 {
		return nil, nil
	}

	fromDelta := fromMs - e.minTS
	toDelta := toMs - e.minTS

	start, _ := slices.BinarySearchFunc(e.deltaTS, fromDelta,
		func(d int32, target int64) int { return cmp.Compare(int64(d), target) })
	end, ok := slices.BinarySearchFunc(e.deltaTS, toDelta,
		func(d int32, target int64) int { return cmp.Compare(int64(d), target) })
	if ok {
		// Include toMs.
		end++
	}

	if start >= end {
		return nil, nil
	}

	tss = make([]int64, end-start)
	for i, d := range e.deltaTS[start:end] {
		tss[i] = e.minTS + int64(d)
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
func (e *MetricsCacheEntry) Append(ts []int64, vals []float64, untilMs int64) uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(ts) == 0 {
		return metricsCacheCost(len(e.deltaTS))
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
			e.deltaTS = append(e.deltaTS, int32(t-e.minTS))
			e.values = append(e.values, vals[i])
			if t > e.maxTS {
				e.maxTS = t
			}
		}
		return metricsCacheCost(len(e.deltaTS))
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
		shift := int32(oldMinTS - newMinTS)
		for i := range e.deltaTS {
			e.deltaTS[i] += shift
		}
		e.minTS = newMinTS

		newDelta := make([]int32, len(prTS)+len(e.deltaTS))
		for i, t := range prTS {
			newDelta[i] = int32(t - newMinTS)
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
		e.deltaTS = append(e.deltaTS, int32(t-e.minTS))
		e.values = append(e.values, vals[splitIdx+i])
		if t > e.maxTS {
			e.maxTS = t
		}
	}

	return metricsCacheCost(len(e.deltaTS))
}

// MarkFetched advances the watermark to record that [fetchFrom, untilMs] has been
// confirmed queried — even when no data points exist in that range.
//
// This lets computeFetchRange treat a series with no data as a cache hit so that
// subsequent queries for the same range skip the ClickHouse round-trip.
func (e *MetricsCacheEntry) MarkFetched(fetchFrom, untilMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.minTS == math.MinInt64 {
		e.minTS = fetchFrom
	} else if fetchFrom < e.minTS {
		shift := int32(e.minTS - fetchFrom)
		for i := range e.deltaTS {
			e.deltaTS[i] += shift
		}
		e.minTS = fetchFrom
	}
	if untilMs > e.maxTS {
		e.maxTS = untilMs
	}
}

// MetricsCacheKey is a key for metrics cache.
type MetricsCacheKey struct {
	Hash [16]byte
	Step int64  // Milliseconds. 0 means raw points.
	Fn   string // aggregation function name; empty means raw/anyLast
}

type metricsCacheStats struct {
	SeriesHits     metric.Int64Counter `name:"metrics_cache.series_hits" description:"Series with a cache watermark covering the full query range." unit:"{series}"`
	SeriesMisses   metric.Int64Counter `name:"metrics_cache.series_misses" description:"Series that required a ClickHouse fetch." unit:"{series}"`
	BigQueries     metric.Int64Counter `name:"metrics_cache.big_queries" description:"Queries where fetched data exceeded maxBytes and the big-query guard fired." unit:"{queries}"`
	SkippedInserts metric.Int64Counter `name:"metrics_cache.skipped_inserts" description:"Series not inserted into cache because a big-query guard was active." unit:"{series}"`
	FullyCovered   metric.Int64Counter `name:"metrics_cache.fully_covered_queries" description:"Queries fully covered by cache with no ClickHouse fetch." unit:"{queries}"`
}

func (s *metricsCacheStats) Init(meter metric.Meter) error {
	return autometric.Init(meter, s, autometric.InitOptions{Prefix: "chstorage."})
}

// MetricsCache wraps otter cache.
type MetricsCache struct {
	cache     otter.Cache[MetricsCacheKey, *MetricsCacheEntry]
	safetyLag time.Duration
	maxBytes  int64

	stats metricsCacheStats
}

// MetricsCacheOptions configures the cache.
type MetricsCacheOptions struct {
	// MaxBytes is max memory budget. Zero disables the cache.
	MaxBytes int64
	// SafetyLag is the duration from now that is not cached.
	SafetyLag time.Duration // default 60s
	// MeterProvider is OpenTelemetry meter provider to use for cache metrics.
	MeterProvider metric.MeterProvider
}

func (opts *MetricsCacheOptions) setDefaults() {
	if opts.SafetyLag <= 0 {
		opts.SafetyLag = time.Minute
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = metricnoop.NewMeterProvider()
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
	builder := otter.MustBuilder[MetricsCacheKey, *MetricsCacheEntry](int(opts.MaxBytes)).
		Cost(func(_ MetricsCacheKey, e *MetricsCacheEntry) uint32 {
			e.mu.RLock()
			defer e.mu.RUnlock()
			return metricsCacheCost(len(e.deltaTS))
		}).
		WithTTL(30 * time.Minute)
	builder = builder.CollectStats()

	cache, err := builder.Build()
	if err != nil {
		return nil, err
	}

	mc := &MetricsCache{
		cache:     cache,
		safetyLag: opts.SafetyLag,
		maxBytes:  opts.MaxBytes,
	}

	meter := opts.MeterProvider.Meter("chstorage.MetricsCache")
	if err := mc.stats.Init(meter); err != nil {
		return nil, errors.Wrap(err, "init metrics")
	}
	if err := registerMetrics(meter, mc); err != nil {
		return nil, errors.Wrap(err, "register metrics")
	}

	return mc, nil
}

// IsBigQuery returns true if the number of points would exceed the cache memory budget.
func (c *MetricsCache) IsBigQuery(totalPoints int) bool {
	return c.maxBytes > 0 && int64(totalPoints)*metricsCachePointCost > c.maxBytes
}

func registerMetrics(meter metric.Meter, mc *MetricsCache) error {
	ratio, err := meter.Float64ObservableGauge("chstorage.metrics_cache.ratio",
		metric.WithDescription("Hit/miss ratio of the metrics cache."),
		metric.WithUnit("{ratio}"))
	if err != nil {
		return err
	}
	hits, err := meter.Int64ObservableCounter("chstorage.metrics_cache.hits",
		metric.WithDescription("Cumulative otter cache.Get hits."))
	if err != nil {
		return err
	}
	misses, err := meter.Int64ObservableCounter("chstorage.metrics_cache.misses",
		metric.WithDescription("Cumulative otter cache.Get misses."))
	if err != nil {
		return err
	}
	size, err := meter.Int64ObservableGauge("chstorage.metrics_cache.size",
		metric.WithDescription("Current number of entries in the cache."),
		metric.WithUnit("{entries}"))
	if err != nil {
		return err
	}
	capacity, err := meter.Int64ObservableGauge("chstorage.metrics_cache.capacity_bytes",
		metric.WithDescription("Configured maximum cache size."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	evictedCount, err := meter.Int64ObservableCounter("chstorage.metrics_cache.evicted_count",
		metric.WithDescription("Cumulative number of evicted entries."))
	if err != nil {
		return err
	}
	evictedCost, err := meter.Int64ObservableCounter("chstorage.metrics_cache.evicted_cost",
		metric.WithDescription("Cumulative cost of evicted entries."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	rejectedSets, err := meter.Int64ObservableCounter("chstorage.metrics_cache.rejected_sets",
		metric.WithDescription("Cumulative number of rejected sets."))
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := mc.cache.Stats()
		observer.ObserveInt64(hits, stats.Hits())
		observer.ObserveInt64(misses, stats.Misses())
		observer.ObserveInt64(size, int64(mc.cache.Size()))
		observer.ObserveFloat64(ratio, stats.Ratio())
		observer.ObserveInt64(capacity, mc.maxBytes)
		observer.ObserveInt64(evictedCount, stats.EvictedCount())
		observer.ObserveInt64(evictedCost, stats.EvictedCost())
		observer.ObserveInt64(rejectedSets, stats.RejectedSets())
		return nil
	}, ratio, hits, misses, size, capacity, evictedCount, evictedCost, rejectedSets)
	return err
}
