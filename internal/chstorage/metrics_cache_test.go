package chstorage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestMetricsCacheEntry(t *testing.T) {
	e := &MetricsCacheEntry{}

	// Initial append
	cost := e.Append([]int64{10, 20, 30}, []float64{1.1, 2.2, 3.3}, 100)
	require.Equal(t, uint32(3*16), cost)
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
	require.Equal(t, uint32(4*16), cost)
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
	e := &MetricsCacheEntry{}
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
	for i := 0; i < 8; i++ {
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
		queryPointsFunc: func(ctx context.Context, table string, s, e time.Time, ts map[[16]byte]labels.Labels) ([]*series[pointData], error) {
			callCount++
			lastStart = s

			// Return data from s to e with 1-minute step
			res := &series[pointData]{labels: labels1}
			for t := s.UnixMilli(); t <= e.UnixMilli(); t += 60000 {
				res.ts = append(res.ts, t)
				res.data.values = append(res.data.values, float64(t))
			}
			return []*series[pointData]{res}, nil
		},
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
