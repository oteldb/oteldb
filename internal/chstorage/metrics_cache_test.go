package chstorage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestMetricsCacheEntry(t *testing.T) {
	e := newMetricsCacheEntry()

	// Initial append
	cost := e.Append([]int64{10, 20, 30}, []float64{1.1, 2.2, 3.3}, 100)
	require.Equal(t, uint32(3*16+128), cost)
	require.Equal(t, int64(10), e.minTS)
	require.Equal(t, int64(30), e.maxTS)

	// Slice part of data
	ts, vals := e.Slice(15, 25)
	require.Equal(t, []int64{20}, ts)
	require.Equal(t, []float64{2.2}, vals)

	// Slice including exact boundaries
	ts, vals = e.Slice(10, 30)
	require.Equal(t, []int64{10, 20, 30}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3}, vals)

	// Append overlapping and new data
	cost = e.Append([]int64{20, 30, 40, 50}, []float64{2.2, 3.3, 4.4, 5.5}, 45)
	require.Equal(t, uint32(4*16+128), cost)
	require.Equal(t, int64(10), e.minTS)
	require.Equal(t, int64(40), e.maxTS)

	ts, vals = e.Slice(0, 100)
	require.Equal(t, []int64{10, 20, 30, 40}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3, 4.4}, vals)

	// Append with untilMs filter
	e.Append([]int64{50, 60}, []float64{5.5, 6.6}, 55)
	require.Equal(t, int64(50), e.maxTS)
}

func TestMetricsCacheEntry_Concurrency(t *testing.T) {
	e := newMetricsCacheEntry()
	var wg sync.WaitGroup

	// Concurrent appends
	for i := range 10 {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				ts := int64(i*1000 + j)
				e.Append([]int64{ts}, []float64{float64(ts)}, 10000)
			}
		})
	}

	// Concurrent slices
	for range 10 {
		wg.Go(func() {
			for range 100 {
				e.Slice(0, 10000)
			}
		})
	}

	wg.Wait()
	require.Greater(t, len(e.timestamps), 0)
}

func TestPromQuerier_QueryPointsCached(t *testing.T) {
	opts := MetricsCacheOptions{
		MaxBytes:  1024 * 1024,
		SafetyLag: time.Minute,
	}
	cache, err := newMetricsCache(opts)
	require.NoError(t, err)

	labels1 := labels.FromStrings("__name__", "test_metric", "job", "test")
	h1 := labels1.Hash()
	var hash1 [16]byte
	// Fill hash1 with some stable value from h1
	for i := range len(hash1) / 2 {
		hash1[i] = byte(h1 >> (i * 8))
	}

	timeseries := map[[16]byte]labels.Labels{
		hash1: labels1,
	}

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	start := now.Add(-10 * time.Minute)
	end := now

	var callCount int
	var lastStart time.Time

	p := &promQuerier{
		metricsCache: cache,
		queryPointsFunc: func(ctx context.Context, table string, s, e time.Time, ts map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			callCount++
			lastStart = s

			// Return data from s to e with 1-minute step
			res := &series[pointData]{labels: labels1}
			for t := s.UnixMilli(); t <= e.UnixMilli(); t += 60000 {
				res.ts = append(res.ts, t)
				res.data.values = append(res.data.values, float64(t))
			}
			return map[[16]byte]*series[pointData]{hash1: res}, nil
		},
		tracer: nooptrace.NewTracerProvider().Tracer("test"),
	}

	t.Run("CacheMiss", func(t *testing.T) {
		callCount = 0
		points, err := p.queryPointsCached(ctx, "points", start, end, timeseries)
		require.NoError(t, err)
		require.Equal(t, 1, callCount)
		require.Equal(t, start.UnixMilli(), lastStart.UnixMilli())
		require.Len(t, points, 1)
		// 11 points (0 to 10 minutes, inclusive)
		require.Len(t, points[0].ts, 11)

		// Verify cache populated (up to safety lag)
		entry, ok := cache.cache.Get(hash1)
		require.True(t, ok)
		require.Greater(t, entry.maxTS, start.UnixMilli())
	})

	t.Run("CacheHit_Partial", func(t *testing.T) {
		callCount = 0
		// Shift end by 2 minutes
		newEnd := end.Add(2 * time.Minute)
		points, err := p.queryPointsCached(ctx, "points", start, newEnd, timeseries)
		require.NoError(t, err)

		require.Equal(t, 1, callCount)
		// Should fetch from watermark + 1
		require.Greater(t, lastStart.UnixMilli(), start.UnixMilli())
		require.Len(t, points, 1)
		require.Len(t, points[0].ts, 13) // 11 + 2 new points
	})
}

// TestQueryPointsCached_WatermarkBeyondEnd verifies that when a cached entry's
// watermark exceeds the query end, points beyond end are not returned.
func TestQueryPointsCached_WatermarkBeyondEnd(t *testing.T) {
	opts := MetricsCacheOptions{
		MaxBytes:  1024 * 1024,
		SafetyLag: time.Second, // small so fixed timestamps are cacheable
	}
	cache, err := newMetricsCache(opts)
	require.NoError(t, err)

	lb := labels.FromStrings("__name__", "m")
	var hash [16]byte
	h := lb.Hash()
	for i := range 8 {
		hash[i] = byte(h >> (i * 8))
	}
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	// Use a fixed epoch far enough in the past so safety lag never interferes.
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-populate cache with data from epoch to epoch+5min (watermark = epoch+5min).
	entry := newMetricsCacheEntry()
	for i := range 6 {
		t := epoch.Add(time.Duration(i) * time.Minute).UnixMilli()
		entry.Append([]int64{t}, []float64{float64(i)}, epoch.Add(10*time.Minute).UnixMilli())
	}
	cache.cache.Set(hash, entry)
	// Watermark is now epoch+5min.
	require.Equal(t, epoch.Add(5*time.Minute).UnixMilli(), entry.maxTS)

	p := &promQuerier{
		metricsCache: cache,
		// Query function should not be called since all data is cached.
		queryPointsFunc: func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			return map[[16]byte]*series[pointData]{}, errors.New("unexpected query")
		},
		tracer: nooptrace.NewTracerProvider().Tracer("test"),
	}

	ctx := context.Background()
	start := epoch
	end := epoch.Add(3 * time.Minute) // end < watermark (epoch+5min)

	points, err := p.queryPointsCached(ctx, "points", start, end, timeseries)
	require.NoError(t, err)
	require.Len(t, points, 1)

	// Only points within [start, end] should be returned: epoch+0..+3min = 4 points.
	for _, ts := range points[0].ts {
		require.LessOrEqual(t, ts, end.UnixMilli(), "point %d is beyond query end %d", ts, end.UnixMilli())
		require.GreaterOrEqual(t, ts, start.UnixMilli())
	}
	require.Len(t, points[0].ts, 4)
}

func TestComputeFetchRange(t *testing.T) {
	cache, err := newMetricsCache(MetricsCacheOptions{MaxBytes: 1024 * 1024})
	require.NoError(t, err)

	h1 := [16]byte{1}
	h2 := [16]byte{2}
	timeseries := map[[16]byte]labels.Labels{
		h1: labels.FromStrings("a", "b"),
		h2: labels.FromStrings("c", "d"),
	}

	start := int64(1000)

	t.Run("AllMisses", func(t *testing.T) {
		wms, fetchStart, stats := computeFetchRange(context.Background(), cache, start, timeseries)
		require.Equal(t, start, fetchStart)
		require.Equal(t, 2, stats.misses)
		require.Equal(t, 0, stats.hits)
		require.Equal(t, int64(uncachedWatermark), wms[h1])
		require.Equal(t, int64(uncachedWatermark), wms[h2])
	})

	t.Run("MixedHitAndMiss", func(t *testing.T) {
		// h1 is a hit at 2000, covering start (1000)
		entry1 := newMetricsCacheEntry()
		entry1.Append([]int64{1000, 2000}, []float64{1, 1}, 3000)
		cache.cache.Set(h1, entry1)

		wms, fetchStart, stats := computeFetchRange(context.Background(), cache, start, timeseries)
		// Even though h1 is a hit, h2 is a miss, so we must fetch from start.
		require.Equal(t, start, fetchStart)
		require.Equal(t, 1, stats.misses)
		require.Equal(t, 1, stats.hits)
		require.Equal(t, int64(2000), wms[h1])
		require.Equal(t, int64(uncachedWatermark), wms[h2])
	})

	t.Run("AllHits", func(t *testing.T) {
		// h2 is a hit at 1500, covering start (1000)
		entry2 := newMetricsCacheEntry()
		entry2.Append([]int64{1000, 1500}, []float64{2, 2}, 3000)
		cache.cache.Set(h2, entry2)

		wms, fetchStart, stats := computeFetchRange(context.Background(), cache, start, timeseries)
		// Both are hits. min(2000, 1500) = 1500. Fetch from 1500 + 1.
		require.Equal(t, int64(1501), fetchStart)
		require.Equal(t, 0, stats.misses)
		require.Equal(t, 2, stats.hits)
		require.Equal(t, int64(2000), wms[h1])
		require.Equal(t, int64(1500), wms[h2])
	})

	t.Run("HitBelowStart", func(t *testing.T) {
		// h3 is a hit but below the requested start
		h3 := [16]byte{3}
		ts3 := map[[16]byte]labels.Labels{h3: labels.FromStrings("e", "f")}
		entry3 := newMetricsCacheEntry()
		entry3.Append([]int64{500}, []float64{3}, 1000)
		cache.cache.Set(h3, entry3)

		wms, fetchStart, stats := computeFetchRange(context.Background(), cache, start, ts3)
		// maxTS (500) < start (1000), so it's a miss for the requested range.
		require.Equal(t, start, fetchStart)
		require.Equal(t, 1, stats.misses)
		require.Equal(t, 0, stats.hits)
		require.Equal(t, int64(uncachedWatermark), wms[h3])
	})
}

func TestQueryPointsCached_GapAtStart(t *testing.T) {
	opts := MetricsCacheOptions{
		MaxBytes:  1024 * 1024,
		SafetyLag: time.Minute,
	}
	cache, err := newMetricsCache(opts)
	require.NoError(t, err)

	lb := labels.FromStrings("__name__", "m")
	var hash [16]byte
	h := lb.Hash()
	for i := range 8 {
		hash[i] = byte(h >> (i * 8))
	}
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	// Use a fixed epoch far enough in the past so safety lag never interferes.
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-populate cache with data from epoch to epoch+5min (watermark = epoch+5min).
	// This represents a previous query for [epoch, epoch+5min].
	entry := newMetricsCacheEntry()
	for i := range 6 {
		t := epoch.Add(time.Duration(i) * time.Minute).UnixMilli()
		entry.Append([]int64{t}, []float64{float64(i)}, epoch.Add(10*time.Minute).UnixMilli())
	}
	cache.cache.Set(hash, entry)
	require.Equal(t, epoch.Add(5*time.Minute).UnixMilli(), entry.maxTS)
	require.Equal(t, epoch.UnixMilli(), entry.minTS)

	p := &promQuerier{
		metricsCache: cache,
		queryPointsFunc: func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			// Return data from s to e with 1-minute step
			res := &series[pointData]{labels: lb}
			for t := s.UnixMilli(); t <= e.UnixMilli(); t += 60000 {
				res.ts = append(res.ts, t)
				res.data.values = append(res.data.values, float64(t))
			}
			return map[[16]byte]*series[pointData]{hash: res}, nil
		},
		tracer: nooptrace.NewTracerProvider().Tracer("test"),
	}

	ctx := context.Background()
	// Query starting BEFORE the cached range.
	start := epoch.Add(-10 * time.Minute)
	end := epoch.Add(10 * time.Minute)

	points, err := p.queryPointsCached(ctx, "points", start, end, timeseries)
	require.NoError(t, err)
	require.Len(t, points, 1)

	// Check for gaps. We expect points from start (epoch-10min) to end (epoch+10min).
	expectedCount := int(end.Sub(start).Minutes()) + 1
	require.Equal(t, expectedCount, len(points[0].ts), "should have %d points, but got %d. Range: [%s, %s]", expectedCount, len(points[0].ts), start, end)

	for i, ts := range points[0].ts {
		expectedTS := start.Add(time.Duration(i) * time.Minute).UnixMilli()
		require.Equal(t, expectedTS, ts, "point %d timestamp mismatch", i)
	}
}
