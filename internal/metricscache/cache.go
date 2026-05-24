package metricscache

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/autometric"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// cacheTTL is the default TTL for in-memory cache entries.
const cacheTTL = 30 * time.Minute

// CacheStats holds OTel counters for cache operation tracking.
type CacheStats struct {
	SeriesHits        metric.Int64Counter `name:"metrics_cache.series_hits" description:"Series with a cache watermark covering the full query range." unit:"{series}"`
	SeriesPartialHits metric.Int64Counter `name:"metrics_cache.series_partial_hits" description:"Series with a cache watermark covering only part of the query range." unit:"{series}"`
	SeriesMisses      metric.Int64Counter `name:"metrics_cache.series_misses" description:"Series that required a ClickHouse fetch." unit:"{series}"`
	BigQueries        metric.Int64Counter `name:"metrics_cache.big_queries" description:"Queries where fetched data exceeded maxBytes and the big-query guard fired." unit:"{queries}"`
	SkippedInserts    metric.Int64Counter `name:"metrics_cache.skipped_inserts" description:"Series not inserted into cache because a big-query guard was active." unit:"{series}"`
	FullyCovered      metric.Int64Counter `name:"metrics_cache.fully_covered_queries" description:"Queries fully covered by cache with no ClickHouse fetch." unit:"{queries}"`
}

func (s *CacheStats) init(meter metric.Meter) error {
	return autometric.Init(meter, s, autometric.InitOptions{Prefix: "chstorage."})
}

// Options configures the Cache.
type Options struct {
	// MaxBytes is the maximum memory budget. Zero disables the cache.
	MaxBytes int64
	// SafetyLag is the duration from now that is not cached.
	SafetyLag time.Duration // default 60s
	// MeterProvider is the OpenTelemetry meter provider for cache metrics.
	MeterProvider metric.MeterProvider
	// Store is the backing store. nil uses a new MemoryStore(MaxBytes).
	Store Store
}

func (opts *Options) setDefaults() {
	if opts.SafetyLag <= 0 {
		opts.SafetyLag = time.Minute
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = metricnoop.NewMeterProvider()
	}
}

func (opts Options) validate() error {
	if opts.MaxBytes < 0 {
		return errors.New("max_bytes must be non-negative")
	}
	if opts.SafetyLag < 0 {
		return errors.New("safety_lag must be non-negative")
	}
	return nil
}

// Cache is the top-level metrics cache.
type Cache struct {
	store     Store
	safetyLag time.Duration
	maxBytes  int64

	// Stats holds OTel metric counters for per-query cache diagnostics.
	Stats CacheStats
}

// New creates a new Cache with the given options.
func New(opts Options) (*Cache, error) {
	opts.setDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	store := opts.Store
	if store == nil {
		var err error
		store, err = NewMemoryStore(opts.MaxBytes)
		if err != nil {
			return nil, errors.Wrap(err, "create memory store")
		}
	}

	c := &Cache{
		store:     store,
		safetyLag: opts.SafetyLag,
		maxBytes:  opts.MaxBytes,
	}

	meter := opts.MeterProvider.Meter("chstorage.MetricsCache")
	if err := c.Stats.init(meter); err != nil {
		return nil, errors.Wrap(err, "init stats")
	}
	if err := c.registerObservableMetrics(meter); err != nil {
		return nil, errors.Wrap(err, "register observable metrics")
	}

	return c, nil
}

// Get retrieves an entry from the cache.
func (c *Cache) Get(key Key) (*Entry, bool) {
	return c.store.Get(key)
}

// Set stores an entry in the cache.
func (c *Cache) Set(key Key, entry *Entry) {
	c.store.Set(key, entry)
}

// SafetyLag returns the duration from now that is not cached.
func (c *Cache) SafetyLag() time.Duration {
	return c.safetyLag
}

// IsBigQuery returns true if the number of points would exceed the cache memory budget.
func (c *Cache) IsBigQuery(totalPoints int) bool {
	return c.maxBytes > 0 && int64(totalPoints)*PointCost > c.maxBytes
}

func (c *Cache) registerObservableMetrics(meter metric.Meter) error {
	ratio, err := meter.Float64ObservableGauge("chstorage.metrics_cache.ratio",
		metric.WithDescription("Hit/miss ratio of the metrics cache."),
		metric.WithUnit("{ratio}"))
	if err != nil {
		return err
	}
	hits, err := meter.Int64ObservableCounter("chstorage.metrics_cache.hits",
		metric.WithDescription("Cumulative store.Get hits."))
	if err != nil {
		return err
	}
	misses, err := meter.Int64ObservableCounter("chstorage.metrics_cache.misses",
		metric.WithDescription("Cumulative store.Get misses."))
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
		st := c.store.Stats()
		observer.ObserveInt64(hits, st.Hits)
		observer.ObserveInt64(misses, st.Misses)
		observer.ObserveInt64(size, int64(st.Size))
		observer.ObserveFloat64(ratio, st.Ratio)
		observer.ObserveInt64(capacity, c.maxBytes)
		observer.ObserveInt64(evictedCount, st.EvictedCount)
		observer.ObserveInt64(evictedCost, st.EvictedCost)
		observer.ObserveInt64(rejectedSets, st.RejectedSets)
		return nil
	}, ratio, hits, misses, size, capacity, evictedCount, evictedCost, rejectedSets)
	return err
}
