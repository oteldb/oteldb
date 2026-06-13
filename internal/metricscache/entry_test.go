package metricscache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricsCacheEntry(t *testing.T) {
	e := NewEntry()

	// Initial append.
	cost := e.append([]int64{10, 20, 30}, []float64{1.1, 2.2, 3.3}, 100)
	require.Equal(t, EntryCost(3), cost)
	minTS, maxTS := e.Watermarks()
	require.Equal(t, int64(10), minTS)
	require.Equal(t, int64(30), maxTS)

	// Slice part of data.
	ts, vals := e.slice(15, 25)
	require.Equal(t, []int64{20}, ts)
	require.Equal(t, []float64{2.2}, vals)

	// Slice including exact boundaries.
	ts, vals = e.slice(10, 30)
	require.Equal(t, []int64{10, 20, 30}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3}, vals)

	// Append overlapping and new data.
	cost = e.append([]int64{20, 30, 40, 50}, []float64{2.2, 3.3, 4.4, 5.5}, 45)
	require.Equal(t, EntryCost(4), cost)
	minTS, maxTS = e.Watermarks()
	require.Equal(t, int64(10), minTS)
	require.Equal(t, int64(40), maxTS)

	ts, vals = e.slice(0, 100)
	require.Equal(t, []int64{10, 20, 30, 40}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3, 4.4}, vals)

	// Append with untilMs filter.
	e.append([]int64{50, 60}, []float64{5.5, 6.6}, 55)
	_, maxTS = e.Watermarks()
	require.Equal(t, int64(50), maxTS)
}

// TestMetricsCacheEntry_Prepend reproduces the bug where querying a wider historical
// range after a shorter query would leave a stale cache that only covered the shorter
// range. Append must prepend older points rather than silently dropping them.
func TestMetricsCacheEntry_Prepend(t *testing.T) {
	e := NewEntry()

	// Simulate first query: 30-minute window [100, 300].
	e.append([]int64{100, 200, 300}, []float64{1.0, 2.0, 3.0}, 300)
	e.markFetched(100, 300)
	minTS, maxTS := e.Watermarks()
	require.Equal(t, int64(100), minTS)
	require.Equal(t, int64(300), maxTS)

	// Simulate second query: 24-hour window [1, 300].
	// The caller fetches [1, 300] from ClickHouse and calls Append with all results.
	// Points older than current minTS (100) must be prepended, not dropped.
	e.append([]int64{1, 50, 100, 200, 300}, []float64{0.1, 0.5, 1.0, 2.0, 3.0}, 300)
	e.markFetched(1, 300)

	minTS, maxTS = e.Watermarks()
	require.Equal(t, int64(1), minTS, "minTS must be updated to the oldest new point")
	require.Equal(t, int64(300), maxTS)

	// Full slice must include the prepended points.
	ts, vals := e.slice(0, 400)
	require.Equal(t, []int64{1, 50, 100, 200, 300}, ts)
	require.Equal(t, []float64{0.1, 0.5, 1.0, 2.0, 3.0}, vals)

	// Third query on the same 24-hour window must be a cache hit returning all data.
	ts, vals = e.slice(1, 300)
	require.Equal(t, []int64{1, 50, 100, 200, 300}, ts)
	require.Equal(t, []float64{0.1, 0.5, 1.0, 2.0, 3.0}, vals)
}

func TestMetricsCacheEntry_Concurrency(t *testing.T) {
	e := NewEntry()
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range 10 {
		wg.Go(func() {
			for j := range 100 {
				ts := int64(i*1000 + j)
				mu.Lock()
				e.append([]int64{ts}, []float64{float64(ts)}, 10000)
				mu.Unlock()
			}
		})
	}
	for range 10 {
		wg.Go(func() {
			for range 100 {
				mu.Lock()
				e.slice(0, 10000)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.Greater(t, e.Len(), 0)
}

// TestMetricsCacheEntry_MarkFetched verifies that markFetched advances the
// watermark for ranges with no data, enabling hit detection in computeFetchRange.
func TestMetricsCacheEntry_MarkFetched(t *testing.T) {
	e := NewEntry()

	e.markFetched(1000, 5000)
	minTS, maxTS := e.Watermarks()
	require.Equal(t, int64(1000), minTS)
	require.Equal(t, int64(5000), maxTS)

	// Appending actual points within the range must not shrink the watermark bounds.
	e.append([]int64{2000, 3000}, []float64{1, 2}, 5000)
	minTS, maxTS = e.Watermarks()
	require.Equal(t, int64(1000), minTS, "minTS should remain at fetchFrom")
	require.Equal(t, int64(5000), maxTS, "maxTS should remain at untilMs")

	// Advancing to a later window should extend maxTS.
	e.markFetched(1000, 8000)
	_, maxTS = e.Watermarks()
	require.Equal(t, int64(8000), maxTS)
}

func TestMetricsCacheEntry_DeltaRange(t *testing.T) {
	e := NewEntry()

	// 30 day range delta in ms: 30 * 24 * 60 * 60 * 1000 = 2_592_000_000 > math.MaxInt32.
	// This would have overflowed the old int32 deltaTS.
	startTS := int64(100000000000)
	endTS := startTS + 30*24*3600*1000

	e.append([]int64{startTS, endTS}, []float64{1.1, 2.2}, endTS)
	minTS, maxTS := e.Watermarks()
	require.Equal(t, startTS, minTS)
	require.Equal(t, endTS, maxTS)

	tss, vals := e.slice(startTS, endTS)
	require.Equal(t, []int64{startTS, endTS}, tss)
	require.Equal(t, []float64{1.1, 2.2}, vals)
}

func TestMetricsCacheEntry_DeltaRange_PrependShift(t *testing.T) {
	e := NewEntry()

	// Use a range that exceeds int32, and test prepend which does shift on deltas.
	startTS := int64(1_000_000_000_000)
	// 40 days > 24.9d
	span := 40 * 24 * time.Hour
	endTS := startTS + span.Milliseconds()

	// First populate with recent data.
	e.append([]int64{startTS + 20*24*time.Hour.Milliseconds(), endTS}, []float64{10, 20}, endTS)

	// Now prepend older data; this exercises shift := oldMin-newMin and += shift on []int64.
	e.append([]int64{startTS, startTS + 10*24*time.Hour.Milliseconds()}, []float64{1, 5}, endTS)

	minTS, maxTS := e.Watermarks()
	require.Equal(t, startTS, minTS)
	require.Equal(t, endTS, maxTS)

	tss, vals := e.slice(startTS, endTS)
	require.Equal(t, []int64{startTS, startTS + 10*24*time.Hour.Milliseconds(), startTS + 20*24*time.Hour.Milliseconds(), endTS}, tss)
	require.Equal(t, []float64{1, 5, 10, 20}, vals)
}
