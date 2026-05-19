package chstorage

import (
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
)

// testEpoch is a fixed point in the past so safety-lag never interferes.
var testEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func TestMetricsCacheEntry(t *testing.T) {
	e := newMetricsCacheEntry()

	// Initial append.
	cost := e.Append([]int64{10, 20, 30}, []float64{1.1, 2.2, 3.3}, 100)
	require.Equal(t, uint32(3*16+128), cost)
	require.Equal(t, int64(10), e.minTS)
	require.Equal(t, int64(30), e.maxTS)

	// Slice part of data.
	ts, vals := e.Slice(15, 25)
	require.Equal(t, []int64{20}, ts)
	require.Equal(t, []float64{2.2}, vals)

	// Slice including exact boundaries.
	ts, vals = e.Slice(10, 30)
	require.Equal(t, []int64{10, 20, 30}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3}, vals)

	// Append overlapping and new data.
	cost = e.Append([]int64{20, 30, 40, 50}, []float64{2.2, 3.3, 4.4, 5.5}, 45)
	require.Equal(t, uint32(4*16+128), cost)
	require.Equal(t, int64(10), e.minTS)
	require.Equal(t, int64(40), e.maxTS)

	ts, vals = e.Slice(0, 100)
	require.Equal(t, []int64{10, 20, 30, 40}, ts)
	require.Equal(t, []float64{1.1, 2.2, 3.3, 4.4}, vals)

	// Append with untilMs filter.
	e.Append([]int64{50, 60}, []float64{5.5, 6.6}, 55)
	require.Equal(t, int64(50), e.maxTS)
}

func TestMetricsCacheEntry_Concurrency(t *testing.T) {
	e := newMetricsCacheEntry()
	var wg sync.WaitGroup

	for i := range 10 {
		wg.Go(func() {
			for j := range 100 {
				ts := int64(i*1000 + j)
				e.Append([]int64{ts}, []float64{float64(ts)}, 10000)
			}
		})
	}
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

// TestMetricsCacheEntry_MarkFetched verifies that MarkFetched advances the
// watermark for ranges with no data, enabling hit detection in computeFetchRange.
func TestMetricsCacheEntry_MarkFetched(t *testing.T) {
	e := newMetricsCacheEntry()

	e.MarkFetched(1000, 5000)
	require.Equal(t, int64(1000), e.minTS)
	require.Equal(t, int64(5000), e.maxTS)

	// Appending actual points within the range must not shrink the watermark bounds.
	e.Append([]int64{2000, 3000}, []float64{1, 2}, 5000)
	require.Equal(t, int64(1000), e.minTS, "minTS should remain at fetchFrom")
	require.Equal(t, int64(5000), e.maxTS, "maxTS should remain at untilMs")

	// Advancing to a later window should extend maxTS.
	e.MarkFetched(1000, 8000)
	require.Equal(t, int64(8000), e.maxTS)
}

func TestComputeFetchRange(t *testing.T) {
	h1 := [16]byte{1}
	h2 := [16]byte{2}
	start := int64(1000)

	for _, tc := range []struct {
		name           string
		timeseries     map[[16]byte]labels.Labels
		setup          func(*MetricsCache)
		wantFetchFrom  int64
		wantHits       int
		wantMisses     int
		wantWatermarks map[[16]byte]int64
	}{
		{
			name: "AllMisses",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
				h2: labels.FromStrings("c", "d"),
			},
			wantFetchFrom:  start,
			wantHits:       0,
			wantMisses:     2,
			wantWatermarks: map[[16]byte]int64{h1: uncachedWatermark, h2: uncachedWatermark},
		},
		{
			name: "SingleHit",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
				h2: labels.FromStrings("c", "d"),
			},
			setup: func(mc *MetricsCache) {
				// h1 is a hit at 2000 covering start (1000); h2 has no entry.
				e := newMetricsCacheEntry()
				e.Append([]int64{1000, 2000}, []float64{1, 1}, 3000)
				mc.cache.Set(MetricsCacheKey{Hash: h1, Step: 0}, e)
			},
			wantFetchFrom:  start, // h2 is a miss — must fetch from start
			wantHits:       1,
			wantMisses:     1,
			wantWatermarks: map[[16]byte]int64{h1: 2000, h2: uncachedWatermark},
		},
		{
			name: "AllHits",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
				h2: labels.FromStrings("c", "d"),
			},
			setup: func(mc *MetricsCache) {
				e1 := newMetricsCacheEntry()
				e1.Append([]int64{1000, 2000}, []float64{1, 1}, 3000)
				mc.cache.Set(MetricsCacheKey{Hash: h1, Step: 0}, e1)

				e2 := newMetricsCacheEntry()
				e2.Append([]int64{1000, 1500}, []float64{2, 2}, 3000)
				mc.cache.Set(MetricsCacheKey{Hash: h2, Step: 0}, e2)
			},
			wantFetchFrom:  1501, // min(2000, 1500) + 1
			wantHits:       2,
			wantMisses:     0,
			wantWatermarks: map[[16]byte]int64{h1: 2000, h2: 1500},
		},
		{
			name: "HitBelowStart",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
			},
			setup: func(mc *MetricsCache) {
				// maxTS (500) < start (1000): entry exists but doesn't cover start.
				e := newMetricsCacheEntry()
				e.Append([]int64{500}, []float64{3}, 1000)
				mc.cache.Set(MetricsCacheKey{Hash: h1, Step: 0}, e)
			},
			wantFetchFrom:  start,
			wantHits:       0,
			wantMisses:     1,
			wantWatermarks: map[[16]byte]int64{h1: uncachedWatermark},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc, err := newMetricsCache(MetricsCacheOptions{MaxBytes: 1024 * 1024})
			require.NoError(t, err)
			if tc.setup != nil {
				tc.setup(mc)
			}
			wms, fetchFrom, stats := computeFetchRange(context.Background(), mc, 0, start, tc.timeseries)
			require.Equal(t, tc.wantFetchFrom, fetchFrom)
			require.Equal(t, tc.wantHits, stats.hits)
			require.Equal(t, tc.wantMisses, stats.misses)
			for h, want := range tc.wantWatermarks {
				require.Equal(t, want, wms[h], "watermark for hash %v", h)
			}
		})
	}
}

// TestQueryPointsCached covers the four cache-state scenarios for queryPointsCached.
// Each case is independent (own cache instance).
func TestQueryPointsCached(t *testing.T) {
	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}
	stepMs := int64(60_000)

	// linearMock returns one data point per minute starting exactly at s.
	// This keeps the arithmetic simple: the first returned timestamp equals s.
	linearMock := func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
		ser := &series[pointData]{labels: lb}
		for t := s.UnixMilli(); t <= e.UnixMilli(); t += stepMs {
			ser.ts = append(ser.ts, t)
			ser.data.values = append(ser.data.values, 1.0)
		}
		return map[[16]byte]*series[pointData]{hash: ser}, nil
	}
	mustNotCall := func(_ context.Context, _ string, _, _ time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
		return nil, errors.New("unexpected ClickHouse query")
	}

	// populateRaw fills the cache with 1 point per minute for [start, end].
	populateRaw := func(mc *MetricsCache, start, end time.Time) {
		e := newMetricsCacheEntry()
		for t := start.UnixMilli(); t <= end.UnixMilli(); t += stepMs {
			e.Append([]int64{t}, []float64{1.0}, end.UnixMilli())
		}
		mc.cache.Set(MetricsCacheKey{Hash: hash, Step: 0}, e)
	}

	epoch := testEpoch
	for _, tc := range []struct {
		name           string
		setup          func(*MetricsCache)
		mockFn         queryPointsFunc
		queryStart     time.Time
		queryEnd       time.Time
		wantCallCount  int
		wantPointCount int // expected number of points in the single result series
	}{
		{
			name:           "NoHit",
			mockFn:         linearMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 11, // epoch+0..+10min inclusive
		},
		{
			// Cache covers [epoch, epoch+5min]; query end extends beyond — only the gap
			// [epoch+5min+1ms, epoch+10min] is fetched.  Merged result: 11 points.
			name: "PartialHit_NewerGap",
			setup: func(mc *MetricsCache) {
				populateRaw(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         linearMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 11, // 6 from cache + 5 from fetch
		},
		{
			// Cache covers [epoch, epoch+5min]; query starts before that range.
			// entryMinTS (epoch) > queryStart (epoch-5min) → full miss → full fetch.
			name: "PartialHit_OlderGap",
			setup: func(mc *MetricsCache) {
				populateRaw(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         linearMock,
			queryStart:     epoch.Add(-5 * time.Minute),
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 16, // epoch-5min..+10min inclusive
		},
		{
			// Cache covers the entire query range; ClickHouse must not be called.
			name: "TotalHit",
			setup: func(mc *MetricsCache) {
				populateRaw(mc, epoch, epoch.Add(10*time.Minute))
			},
			mockFn:         mustNotCall,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  0,
			wantPointCount: 11,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc, err := newMetricsCache(MetricsCacheOptions{
				MaxBytes:  1024 * 1024,
				SafetyLag: time.Minute,
			})
			require.NoError(t, err)
			if tc.setup != nil {
				tc.setup(mc)
			}

			callCount := 0
			p := &promQuerier{
				metricsCache: mc,
				tracer:       nooptrace.NewTracerProvider().Tracer("test"),
				queryPointsFunc: func(ctx context.Context, table string, s, e time.Time, ts map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
					callCount++
					return tc.mockFn(ctx, table, s, e, ts)
				},
			}

			pts, err := p.queryPointsCached(context.Background(), "points", tc.queryStart, tc.queryEnd, timeseries)
			require.NoError(t, err)
			require.Equal(t, tc.wantCallCount, callCount, "ClickHouse call count")
			if tc.wantPointCount == 0 {
				require.Empty(t, pts)
			} else {
				require.Len(t, pts, 1)
				require.Len(t, pts[0].ts, tc.wantPointCount, "point count")
			}
		})
	}
}

// TestQueryPointsCached_AbsentSeries verifies that a series with no data from
// ClickHouse is cached as empty so subsequent queries skip the round-trip.
func TestQueryPointsCached_AbsentSeries(t *testing.T) {
	mc, err := newMetricsCache(MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Second})
	require.NoError(t, err)

	lb := labels.FromStrings("__name__", "absent")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}
	epoch := testEpoch

	callCount := 0
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		queryPointsFunc: func(_ context.Context, _ string, _, _ time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			callCount++
			return map[[16]byte]*series[pointData]{}, nil
		},
	}

	ctx := context.Background()
	pts, err := p.queryPointsCached(ctx, "points", epoch, epoch.Add(5*time.Minute), timeseries)
	require.NoError(t, err)
	require.Equal(t, 1, callCount)
	require.Empty(t, pts)

	callCount = 0
	pts, err = p.queryPointsCached(ctx, "points", epoch, epoch.Add(5*time.Minute), timeseries)
	require.NoError(t, err)
	require.Equal(t, 0, callCount, "second query must hit the empty-series cache")
	require.Empty(t, pts)
}

// TestQuerySampledPointsCached is a 4×4 combinatorial test: every cache scenario
// is exercised against every grouping configuration.
//
// Three series are used so that all four grouping modes produce distinguishable
// results:
//
//	lb1 {job=a, instance=1, ns=prod}
//	lb2 {job=a, instance=2, ns=prod}
//	lb3 {job=b, instance=1, ns=prod}
//
// All mock functions return value=1.0 per point, so the total sum across all
// output series is always 3×pointsPerSeries, regardless of grouping.
func TestQuerySampledPointsCached(t *testing.T) {
	lb1 := labels.FromStrings("job", "a", "instance", "1", "ns", "prod")
	lb2 := labels.FromStrings("job", "a", "instance", "2", "ns", "prod")
	lb3 := labels.FromStrings("job", "b", "instance", "1", "ns", "prod")

	hash1 := labelHash(lb1.Hash())
	hash2 := labelHash(lb2.Hash())
	hash3 := labelHash(lb3.Hash())

	allHashes := [3][16]byte{hash1, hash2, hash3}
	timeseries := map[[16]byte]labels.Labels{
		hash1: lb1,
		hash2: lb2,
		hash3: lb3,
	}

	epoch := testEpoch
	step := time.Minute
	stepMs := step.Milliseconds()

	// snapToStep rounds ms up to the nearest step boundary relative to epoch,
	// matching the tumble-window alignment used by querySampledPointsPerSeries.
	snapToStep := func(ms int64) int64 {
		epochMs := epoch.UnixMilli()
		offset := (ms - epochMs) % stepMs
		if offset < 0 {
			offset += stepMs
		}
		if offset == 0 {
			return ms
		}
		return ms + stepMs - offset
	}

	// sampledMock returns one step-aligned point (value=1.0) per series per step.
	sampledMock := func(_ context.Context, _ pointsSampler, s, e time.Time, _ time.Duration, ts map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
		res := make(map[[16]byte]*series[pointData], len(ts))
		firstMs := snapToStep(s.UnixMilli())
		for h, lb := range ts {
			ser := &series[pointData]{labels: lb}
			for t := firstMs; t <= e.UnixMilli(); t += stepMs {
				ser.ts = append(ser.ts, t)
				ser.data.values = append(ser.data.values, 1.0)
			}
			res[h] = ser
		}
		return res, nil
	}
	mustNotCall := func(_ context.Context, _ pointsSampler, _, _ time.Time, _ time.Duration, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
		return nil, errors.New("unexpected ClickHouse query")
	}

	// populateSampled fills the cache for all three series with step-aligned points.
	populateSampled := func(mc *MetricsCache, start, end time.Time) {
		for _, h := range allHashes {
			e := newMetricsCacheEntry()
			for t := start.UnixMilli(); t <= end.UnixMilli(); t += stepMs {
				e.Append([]int64{t}, []float64{1.0}, end.UnixMilli())
			}
			mc.cache.Set(MetricsCacheKey{Hash: h, Step: stepMs}, e)
		}
	}

	// sumAllValues returns the sum of every data value across all series.
	// Regardless of grouping, this always equals 3 × pointsPerSeries.
	sumAllValues := func(pts []*series[pointData]) float64 {
		var sum float64
		for _, s := range pts {
			for _, v := range s.data.values {
				sum += v
			}
		}
		return sum
	}

	type cacheScenario struct {
		name           string
		setup          func(*MetricsCache)
		mockFn         querySampledPointsPerSeriesFunc
		queryStart     time.Time
		queryEnd       time.Time
		wantCallCount  int
		wantPointsEach int // expected points per original series before grouping
	}
	cacheScenarios := []cacheScenario{
		{
			name:           "NoHit",
			mockFn:         sampledMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointsEach: 11, // epoch+0..+10min inclusive
		},
		{
			// Cache covers [epoch, epoch+5min]; query extends to epoch+10min.
			// Only the newer gap is fetched.
			name: "PartialHit_NewerGap",
			setup: func(mc *MetricsCache) {
				populateSampled(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         sampledMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointsEach: 11, // 6 from cache (epoch..+5min) + 5 fetched (+6..+10min)
		},
		{
			// Cache covers [epoch, epoch+5min]; query starts before that.
			// entryMinTS (epoch) > queryStart (epoch-5min) → full miss.
			name: "PartialHit_OlderGap",
			setup: func(mc *MetricsCache) {
				populateSampled(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         sampledMock,
			queryStart:     epoch.Add(-5 * time.Minute),
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointsEach: 16, // epoch-5min..+10min inclusive
		},
		{
			// Cache covers the full query range; ClickHouse must not be called.
			name: "TotalHit",
			setup: func(mc *MetricsCache) {
				populateSampled(mc, epoch, epoch.Add(10*time.Minute))
			},
			mockFn:         mustNotCall,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  0,
			wantPointsEach: 11,
		},
	}

	type groupingScenario struct {
		name        string
		on          bool
		groupBy     []string
		wantGroups  int
		checkGroups func(t *testing.T, pts []*series[pointData], pointsEach int)
	}
	groupingScenarios := []groupingScenario{
		{
			// No aggregation: three separate series, each value=1.0/pt.
			name:       "NoGrouping",
			wantGroups: 3,
			checkGroups: func(t *testing.T, pts []*series[pointData], pointsEach int) {
				require.InDelta(t, float64(3*pointsEach), sumAllValues(pts), 1e-9)
				for _, s := range pts {
					require.Len(t, s.ts, pointsEach)
					for _, v := range s.data.values {
						require.InDelta(t, 1.0, v, 1e-9)
					}
				}
			},
		},
		{
			// by (job): two groups — job=a (lb1+lb2, value=2.0) and job=b (lb3, value=1.0).
			name:       "SingleLabel_MultiValue",
			on:         true,
			groupBy:    []string{"job"},
			wantGroups: 2,
			checkGroups: func(t *testing.T, pts []*series[pointData], pointsEach int) {
				require.InDelta(t, float64(3*pointsEach), sumAllValues(pts), 1e-9)
				for _, s := range pts {
					require.Len(t, s.ts, pointsEach)
					job := s.labels.Get("job")
					wantVal := map[string]float64{"a": 2.0, "b": 1.0}[job]
					for _, v := range s.data.values {
						require.InDelta(t, wantVal, v, 1e-9, "job=%s", job)
					}
				}
			},
		},
		{
			// by (ns): all three series share ns=prod → collapses to one group, value=3.0/pt.
			name:       "SingleLabel_AllSameValue",
			on:         true,
			groupBy:    []string{"ns"},
			wantGroups: 1,
			checkGroups: func(t *testing.T, pts []*series[pointData], pointsEach int) {
				require.Equal(t, "prod", pts[0].labels.Get("ns"))
				require.Len(t, pts[0].ts, pointsEach)
				require.InDelta(t, float64(3*pointsEach), sumAllValues(pts), 1e-9)
				for _, v := range pts[0].data.values {
					require.InDelta(t, 3.0, v, 1e-9)
				}
			},
		},
		{
			// by (job, instance): each (job,instance) pair is unique → three groups, value=1.0/pt.
			name:       "MultiLabel",
			on:         true,
			groupBy:    []string{"job", "instance"},
			wantGroups: 3,
			checkGroups: func(t *testing.T, pts []*series[pointData], pointsEach int) {
				require.InDelta(t, float64(3*pointsEach), sumAllValues(pts), 1e-9)
				for _, s := range pts {
					require.Len(t, s.ts, pointsEach)
					require.NotEmpty(t, s.labels.Get("job"))
					require.NotEmpty(t, s.labels.Get("instance"))
					for _, v := range s.data.values {
						require.InDelta(t, 1.0, v, 1e-9)
					}
				}
			},
		},
	}

	sampler := pointsSampler{needPoints: true, pointExpr: chsql.LastValue}
	for _, cs := range cacheScenarios {
		for _, gs := range groupingScenarios {
			cs, gs := cs, gs
			t.Run(cs.name+"/"+gs.name, func(t *testing.T) {
				mc, err := newMetricsCache(MetricsCacheOptions{
					MaxBytes:  1024 * 1024,
					SafetyLag: time.Minute,
				})
				require.NoError(t, err)
				if cs.setup != nil {
					cs.setup(mc)
				}

				callCount := 0
				p := &promQuerier{
					metricsCache: mc,
					tables:       Tables{Points: "points"},
					tracer:       nooptrace.NewTracerProvider().Tracer("test"),
					querySampledPointsPerSeriesFunc: func(ctx context.Context, s pointsSampler, start, end time.Time, step time.Duration, ts map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
						callCount++
						return cs.mockFn(ctx, s, start, end, step, ts)
					},
				}

				pts, err := p.querySampledPointsCached(context.Background(), sampler, gs.on, gs.groupBy, cs.queryStart, cs.queryEnd, step, timeseries)
				require.NoError(t, err)
				require.Equal(t, cs.wantCallCount, callCount, "ClickHouse call count")
				require.Len(t, pts, gs.wantGroups, "group count")
				gs.checkGroups(t, pts, cs.wantPointsEach)

				// Ensure the labels contain exactly the expected label set for the
				// grouping (no leakage of ungrouped labels or missing grouped labels).
				for _, s := range pts {
					if !gs.on {
						continue
					}
					for _, key := range gs.groupBy {
						require.NotEmpty(t, s.labels.Get(key), "grouped label %q missing", key)
					}
					for _, key := range []string{"job", "instance", "ns"} {
						if !slices.Contains(gs.groupBy, key) {
							require.Empty(t, s.labels.Get(key), "ungrouped label %q should be absent", key)
						}
					}
				}
			})
		}
	}
}

// TestCacheKeyStepIsolation verifies that cached data for one step value is never
// served for a query with a different step value.
func TestCacheKeyStepIsolation(t *testing.T) {
	mc, err := newMetricsCache(MetricsCacheOptions{MaxBytes: 1024 * 1024})
	require.NoError(t, err)

	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	epoch := testEpoch
	start := epoch
	end := epoch.Add(10 * time.Minute)
	stepMin := time.Minute
	step5Min := 5 * time.Minute

	// Populate cache at step=1min.
	entry := newMetricsCacheEntry()
	for i := range 11 {
		entry.Append([]int64{epoch.Add(time.Duration(i) * time.Minute).UnixMilli()}, []float64{float64(i)}, end.UnixMilli())
	}
	mc.cache.Set(MetricsCacheKey{Hash: hash, Step: stepMin.Milliseconds()}, entry)

	callCount := 0
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		querySampledPointsPerSeriesFunc: func(_ context.Context, _ pointsSampler, _, _ time.Time, _ time.Duration, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			callCount++
			return map[[16]byte]*series[pointData]{}, nil
		},
	}

	sampler := pointsSampler{needPoints: true, pointExpr: chsql.LastValue}
	_, err = p.querySampledPointsCached(context.Background(), sampler, false, nil, start, end, step5Min, timeseries)
	require.NoError(t, err)
	require.Equal(t, 1, callCount, "step=5min cache is empty: ClickHouse must be called")

	callCount = 0
	_, err = p.querySampledPointsCached(context.Background(), sampler, false, nil, start, end, stepMin, timeseries)
	require.NoError(t, err)
	require.Equal(t, 0, callCount, "step=1min is fully cached: ClickHouse must not be called")
}

func labelHash(v uint64) (r [16]byte) {
	binary.LittleEndian.PutUint64(r[:], v)
	return r
}
