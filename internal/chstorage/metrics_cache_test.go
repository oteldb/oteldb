package chstorage

import (
	"context"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/metricscache"
)

// testEpoch is a fixed point in the past so safety-lag never interferes.
var testEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestCache(t *testing.T, opts metricscache.Options) *metricscache.Cache {
	t.Helper()
	mc, err := metricscache.New(opts)
	require.NoError(t, err)
	return mc
}

func TestComputeFetchRange(t *testing.T) {
	h1 := [16]byte{1}
	h2 := [16]byte{2}
	start := int64(1000)

	for _, tc := range []struct {
		name           string
		timeseries     map[[16]byte]labels.Labels
		setup          func(*metricscache.Cache)
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
			setup: func(mc *metricscache.Cache) {
				// h1 is a hit at 2000 covering start (1000); h2 has no entry.
				mc.Update(metricscache.Key{Hash: h1, Step: 0}, metricscache.Update{
					FetchFrom: 1000,
					UntilMs:   3000,
					TS:        []int64{1000, 2000},
					Vals:      []float64{1, 1},
				})
			},
			wantFetchFrom:  start, // h2 is a miss — must fetch from start
			wantHits:       1,
			wantMisses:     1,
			wantWatermarks: map[[16]byte]int64{h1: 3000, h2: uncachedWatermark},
		},
		{
			name: "AllHits",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
				h2: labels.FromStrings("c", "d"),
			},
			setup: func(mc *metricscache.Cache) {
				mc.Update(metricscache.Key{Hash: h1, Step: 0}, metricscache.Update{
					FetchFrom: 1000,
					UntilMs:   3000,
					TS:        []int64{1000, 2000},
					Vals:      []float64{1, 1},
				})

				mc.Update(metricscache.Key{Hash: h2, Step: 0}, metricscache.Update{
					FetchFrom: 1000,
					UntilMs:   3000,
					TS:        []int64{1000, 1500},
					Vals:      []float64{2, 2},
				})
			},
			wantFetchFrom:  3001, // min(3000, 3000) + 1
			wantHits:       2,
			wantMisses:     0,
			wantWatermarks: map[[16]byte]int64{h1: 3000, h2: 3000},
		},
		{
			name: "HitBelowStart",
			timeseries: map[[16]byte]labels.Labels{
				h1: labels.FromStrings("a", "b"),
			},
			setup: func(mc *metricscache.Cache) {
				// maxTS (500) < start (1000): entry exists but doesn't cover start.
				mc.Update(metricscache.Key{Hash: h1, Step: 0}, metricscache.Update{
					FetchFrom: 500,
					UntilMs:   500,
					TS:        []int64{500},
					Vals:      []float64{3},
				})
			},
			wantFetchFrom:  start,
			wantHits:       0,
			wantMisses:     1,
			wantWatermarks: map[[16]byte]int64{h1: uncachedWatermark},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024})
			if tc.setup != nil {
				tc.setup(mc)
			}
			wms, fetchFrom, stats := computeFetchRange(context.Background(), mc, 0, "", start, tc.timeseries)
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
	populateRaw := func(mc *metricscache.Cache, start, end time.Time) {
		var ts []int64
		var vals []float64
		for t := start.UnixMilli(); t <= end.UnixMilli(); t += stepMs {
			ts = append(ts, t)
			vals = append(vals, 1.0)
		}
		mc.Update(metricscache.Key{Hash: hash, Step: 0}, metricscache.Update{
			FetchFrom: start.UnixMilli(),
			UntilMs:   end.UnixMilli(),
			TS:        ts,
			Vals:      vals,
		})
	}

	epoch := testEpoch
	for _, tc := range []struct {
		name           string
		setup          func(*metricscache.Cache)
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
			setup: func(mc *metricscache.Cache) {
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
			setup: func(mc *metricscache.Cache) {
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
			setup: func(mc *metricscache.Cache) {
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
			mc := newTestCache(t, MetricsCacheOptions{
				MaxBytes:  1024 * 1024,
				SafetyLag: time.Minute,
			})
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
	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Second})

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

// TestQueryPointsCached_BigQuery verifies the big-query guard: when the number of
// fetched points would exceed MaxBytes, upsertCache must not create new entries but
// must still refresh entries that are already in the cache.
func TestQueryPointsCached_BigQuery(t *testing.T) {
	epoch := testEpoch

	lb1 := labels.FromStrings("__name__", "m", "instance", "1")
	lb2 := labels.FromStrings("__name__", "m", "instance", "2")
	hash1 := labelHash(lb1.Hash())
	hash2 := labelHash(lb2.Hash())
	timeseries := map[[16]byte]labels.Labels{hash1: lb1, hash2: lb2}

	stepMs := int64(60_000)
	// mock returns 100 points per series → totalPoints=200, totalBytes=3200.
	mockFn := func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
		build := func(lb labels.Labels) *series[pointData] {
			ser := &series[pointData]{labels: lb}
			for t := s.UnixMilli(); t <= e.UnixMilli(); t += stepMs {
				ser.ts = append(ser.ts, t)
				ser.data.values = append(ser.data.values, 1.0)
			}
			return ser
		}
		return map[[16]byte]*series[pointData]{
			hash1: build(lb1),
			hash2: build(lb2),
		}, nil
	}

	// MaxBytes is between one-series cost (100*16+128=1728) and two-series cost (3200)
	// so that the guard fires (3200 > 2048) but a single updated entry still fits.
	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 2048, SafetyLag: time.Second})

	// Pre-warm only hash1 in the cache.
	mc.Update(metricscache.Key{Hash: hash1}, metricscache.Update{
		FetchFrom: epoch.UnixMilli(),
		UntilMs:   epoch.UnixMilli(),
		TS:        []int64{epoch.UnixMilli()},
		Vals:      []float64{0.0},
	})

	p := &promQuerier{
		metricsCache:    mc,
		tracer:          nooptrace.NewTracerProvider().Tracer("test"),
		queryPointsFunc: mockFn,
	}

	// Query triggers the big-query guard (fetched >> MaxBytes=256).
	pts, err := p.queryPointsCached(context.Background(), "points", epoch, epoch.Add(99*time.Minute), timeseries)
	require.NoError(t, err)
	require.NotEmpty(t, pts, "results should still be returned even for a big query")

	// hash1 was already cached: its entry must have been updated.
	_, hit1 := mc.Lookup(metricscache.Key{Hash: hash1}, epoch.UnixMilli())
	require.True(t, hit1, "pre-warmed hash1 must still be in cache after big query")
	ts1, _, ok1 := mc.Read(metricscache.Key{Hash: hash1}, epoch.UnixMilli(), epoch.Add(99*time.Minute).UnixMilli())
	require.True(t, ok1)
	require.NotEmpty(t, ts1, "hash1 cache entry must have been refreshed with new points")

	// hash2 was NOT cached before the query: the guard must have prevented its insertion.
	_, hit2 := mc.Lookup(metricscache.Key{Hash: hash2}, epoch.UnixMilli())
	require.False(t, hit2, "uncached hash2 must NOT be inserted during a big query")
}

// TestQueryPointsCached_DisjointGap is a regression test for the watermark gap bug:
// if a "partial hit" causes upsertCache to be called with fetchFrom > entry.maxTS, the
// old entry must be reset rather than extended.  Before the fix, MarkFetched would
// stretch maxTS over the unfetched gap, making a subsequent query falsely appear as a
// TotalHit and silently omit the missing range from the results.
func TestQueryPointsCached_DisjointGap(t *testing.T) {
	epoch := testEpoch
	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}
	stepMs := int64(60_000)

	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Minute})
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		queryPointsFunc: func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			ser := &series[pointData]{labels: lb}
			for t := s.UnixMilli(); t <= e.UnixMilli(); t += stepMs {
				ser.ts = append(ser.ts, t)
				ser.data.values = append(ser.data.values, 1.0)
			}
			return map[[16]byte]*series[pointData]{hash: ser}, nil
		},
	}
	ctx := context.Background()

	// Step 1: populate cache for [epoch, epoch+5min].
	_, err := p.queryPointsCached(ctx, "points", epoch, epoch.Add(5*time.Minute), timeseries)
	require.NoError(t, err)

	// Step 2: query a disjoint newer range [epoch+10min, epoch+15min].
	// This is a partial hit (entryMinTS <= start but entryMaxTS < start), so it
	// fetches from epoch+10min and calls upsertCache with fetchFrom > oldMaxTS.
	_, err = p.queryPointsCached(ctx, "points", epoch.Add(10*time.Minute), epoch.Add(15*time.Minute), timeseries)
	require.NoError(t, err)

	// Step 3: query the full range [epoch, epoch+15min].
	// Before the fix the cache entry would falsely claim watermarks [epoch, epoch+14min]
	// (the gap [epoch+5min, epoch+10min] was never fetched), resulting in only the
	// last minute being re-fetched and data for the 5-minute gap being silently missing.
	// After the fix the entry is reset on the disjoint upsert, so this query is treated
	// as a miss and the full 16 minutes are fetched from ClickHouse.
	pts, err := p.queryPointsCached(ctx, "points", epoch, epoch.Add(15*time.Minute), timeseries)
	require.NoError(t, err)
	require.Len(t, pts, 1)
	require.Len(t, pts[0].ts, 16, "all 16 minutes must be present — no gap in [epoch+5min, epoch+10min]")
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
	populateSampled := func(mc *metricscache.Cache, start, end time.Time) {
		for _, h := range allHashes {
			var ts []int64
			var vals []float64
			for t := start.UnixMilli(); t <= end.UnixMilli(); t += stepMs {
				ts = append(ts, t)
				vals = append(vals, 1.0)
			}
			mc.Update(metricscache.Key{Hash: h, Step: stepMs, Fn: "sum"}, metricscache.Update{
				FetchFrom: start.UnixMilli(),
				UntilMs:   end.UnixMilli(),
				TS:        ts,
				Vals:      vals,
			})
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
		setup          func(*metricscache.Cache)
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
			setup: func(mc *metricscache.Cache) {
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
			setup: func(mc *metricscache.Cache) {
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
			setup: func(mc *metricscache.Cache) {
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

	sampler := pointsSampler{needPoints: true, pointExpr: chsql.LastValue, agg: sumAggregator, fn: "sum"}
	for _, cs := range cacheScenarios {
		for _, gs := range groupingScenarios {
			cs, gs := cs, gs
			t.Run(cs.name+"/"+gs.name, func(t *testing.T) {
				mc := newTestCache(t, MetricsCacheOptions{
					MaxBytes:  1024 * 1024,
					SafetyLag: time.Minute,
				})
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
	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024})

	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	epoch := testEpoch
	start := epoch
	end := epoch.Add(10 * time.Minute)
	stepMin := time.Minute
	step5Min := 5 * time.Minute

	// Populate cache at step=1min.
	var ts []int64
	var vals []float64
	for i := range 11 {
		ts = append(ts, epoch.Add(time.Duration(i)*time.Minute).UnixMilli())
		vals = append(vals, float64(i))
	}
	mc.Update(metricscache.Key{Hash: hash, Step: stepMin.Milliseconds()}, metricscache.Update{
		FetchFrom: epoch.UnixMilli(),
		UntilMs:   end.UnixMilli(),
		TS:        ts,
		Vals:      vals,
	})

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
	_, err := p.querySampledPointsCached(context.Background(), sampler, false, nil, start, end, step5Min, timeseries)
	require.NoError(t, err)
	require.Equal(t, 1, callCount, "step=5min cache is empty: ClickHouse must be called")

	callCount = 0
	_, err = p.querySampledPointsCached(context.Background(), sampler, false, nil, start, end, stepMin, timeseries)
	require.NoError(t, err)
	require.Equal(t, 0, callCount, "step=1min is fully cached: ClickHouse must not be called")
}

// TestQueryRatePointsCached covers the four cache-state scenarios for queryRatePointsCached.
// Each case is independent (own cache instance).
func TestQueryRatePointsCached(t *testing.T) {
	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	epoch := testEpoch
	step := time.Minute
	stepMs := step.Milliseconds()
	window := 5 * time.Minute

	// rateMock returns one pre-computed rate value per step starting from fetchStart.
	rateMock := func(_ context.Context, s, e time.Time, _, _, _ time.Duration, _ map[[16]byte]labels.Labels, _ rateKind) (map[[16]byte]*series[pointData], error) {
		ser := &series[pointData]{labels: lb}
		for t := s.UnixMilli(); t <= e.UnixMilli(); t += stepMs {
			ser.ts = append(ser.ts, t)
			ser.data.values = append(ser.data.values, 1.0)
		}
		return map[[16]byte]*series[pointData]{hash: ser}, nil
	}
	mustNotCall := func(_ context.Context, _, _ time.Time, _, _, _ time.Duration, _ map[[16]byte]labels.Labels, _ rateKind) (map[[16]byte]*series[pointData], error) {
		return nil, errors.New("unexpected ClickHouse query")
	}

	// rateCacheKey returns the cache key used by queryRatePointsCached.
	rateCacheKey := func(kind rateKind, step, window, offset time.Duration) metricscache.Key {
		return metricscache.Key{Hash: hash, Step: step.Milliseconds(), Fn: rateCacheFn(kind, window, offset)}
	}

	// populateRate fills the cache for the series with step-aligned rate values.
	populateRate := func(mc *metricscache.Cache, start, end time.Time) {
		var ts []int64
		var vals []float64
		for t := start.UnixMilli(); t <= end.UnixMilli(); t += stepMs {
			ts = append(ts, t)
			vals = append(vals, 1.0)
		}
		mc.Update(rateCacheKey(rateKindRate, step, window, 0), metricscache.Update{
			FetchFrom: start.UnixMilli(),
			UntilMs:   end.UnixMilli(),
			TS:        ts,
			Vals:      vals,
		})
	}

	for _, tc := range []struct {
		name           string
		setup          func(*metricscache.Cache)
		mockFn         queryRatePointsByHashFunc
		queryStart     time.Time
		queryEnd       time.Time
		wantCallCount  int
		wantPointCount int
	}{
		{
			name:           "NoHit",
			mockFn:         rateMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 11, // epoch+0..+10min inclusive
		},
		{
			// Cache covers [epoch, epoch+5min]; query extends to epoch+10min.
			// Only the newer gap is fetched.
			name: "PartialHit_NewerGap",
			setup: func(mc *metricscache.Cache) {
				populateRate(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         rateMock,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 11, // 6 from cache + 5 fetched
		},
		{
			// Cache covers [epoch, epoch+5min]; query starts before that.
			// entryMinTS (epoch) > queryStart (epoch-5min) → full miss.
			name: "PartialHit_OlderGap",
			setup: func(mc *metricscache.Cache) {
				populateRate(mc, epoch, epoch.Add(5*time.Minute))
			},
			mockFn:         rateMock,
			queryStart:     epoch.Add(-5 * time.Minute),
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  1,
			wantPointCount: 16, // epoch-5min..+10min inclusive
		},
		{
			// Cache covers the full query range; ClickHouse must not be called.
			name: "TotalHit",
			setup: func(mc *metricscache.Cache) {
				populateRate(mc, epoch, epoch.Add(10*time.Minute))
			},
			mockFn:         mustNotCall,
			queryStart:     epoch,
			queryEnd:       epoch.Add(10 * time.Minute),
			wantCallCount:  0,
			wantPointCount: 11,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newTestCache(t, MetricsCacheOptions{
				MaxBytes:  1024 * 1024,
				SafetyLag: time.Minute,
			})
			if tc.setup != nil {
				tc.setup(mc)
			}

			callCount := 0
			p := &promQuerier{
				metricsCache: mc,
				tracer:       nooptrace.NewTracerProvider().Tracer("test"),
				queryRatePointsByHashFunc: func(ctx context.Context, s, e time.Time, step, window, offset time.Duration, ts map[[16]byte]labels.Labels, kind rateKind) (map[[16]byte]*series[pointData], error) {
					callCount++
					return tc.mockFn(ctx, s, e, step, window, offset, ts, kind)
				},
			}

			pts, err := p.queryRatePointsCached(context.Background(), tc.queryStart, tc.queryEnd, step, window, 0, timeseries, rateKindRate)
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

func TestQueryRatePointsCached_WindowOffsetIsolation(t *testing.T) {
	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	epoch := testEpoch
	start := epoch
	end := epoch.Add(10 * time.Minute)
	step := time.Minute

	for _, tc := range []struct {
		name       string
		firstWin   time.Duration
		firstOff   time.Duration
		secondWin  time.Duration
		secondOff  time.Duration
		firstValue float64
		wantValue  float64
	}{
		{
			name:       "Window",
			firstWin:   5 * time.Minute,
			secondWin:  10 * time.Minute,
			firstValue: 5,
			wantValue:  10,
		},
		{
			name:       "Offset",
			firstWin:   5 * time.Minute,
			secondWin:  5 * time.Minute,
			secondOff:  2 * time.Minute,
			firstValue: 5,
			wantValue:  7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Minute})
			callCount := 0
			p := &promQuerier{
				metricsCache: mc,
				tracer:       nooptrace.NewTracerProvider().Tracer("test"),
				queryRatePointsByHashFunc: func(_ context.Context, s, e time.Time, step, window, offset time.Duration, _ map[[16]byte]labels.Labels, _ rateKind) (map[[16]byte]*series[pointData], error) {
					callCount++
					ser := &series[pointData]{labels: lb}
					value := float64(window/time.Minute + offset/time.Minute)
					for ts := s.UnixMilli(); ts <= e.UnixMilli(); ts += step.Milliseconds() {
						ser.ts = append(ser.ts, ts)
						ser.data.values = append(ser.data.values, value)
					}
					return map[[16]byte]*series[pointData]{hash: ser}, nil
				},
			}

			pts, err := p.queryRatePointsCached(context.Background(), start, end, step, tc.firstWin, tc.firstOff, timeseries, rateKindRate)
			require.NoError(t, err)
			require.Equal(t, 1, callCount)
			require.Len(t, pts, 1)
			require.NotEmpty(t, pts[0].data.values)
			require.Equal(t, tc.firstValue, pts[0].data.values[0])

			callCount = 0
			pts, err = p.queryRatePointsCached(context.Background(), start, end, step, tc.secondWin, tc.secondOff, timeseries, rateKindRate)
			require.NoError(t, err)
			require.Equal(t, 1, callCount, "different rate window/offset must not hit the previous cache entry")
			require.Len(t, pts, 1)
			require.NotEmpty(t, pts[0].data.values)
			require.Equal(t, tc.wantValue, pts[0].data.values[0])

			// Same parameters should now be served from the second cache entry.
			callCount = 0
			_, err = p.queryRatePointsCached(context.Background(), start, end, step, tc.secondWin, tc.secondOff, timeseries, rateKindRate)
			require.NoError(t, err)
			require.Equal(t, 0, callCount)
		})
	}
}

func TestQueryRatePointsCached_PartialHitKeepsStepAlignment(t *testing.T) {
	lb := labels.FromStrings("__name__", "m")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}

	epoch := testEpoch
	step := time.Minute
	stepMs := step.Milliseconds()
	window := 5 * time.Minute

	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Minute})
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		queryRatePointsByHashFunc: func(_ context.Context, s, e time.Time, step, _, _ time.Duration, _ map[[16]byte]labels.Labels, _ rateKind) (map[[16]byte]*series[pointData], error) {
			ser := &series[pointData]{labels: lb}
			for ts := s.UnixMilli(); ts <= e.UnixMilli(); ts += step.Milliseconds() {
				ser.ts = append(ser.ts, ts)
				ser.data.values = append(ser.data.values, 1)
			}
			return map[[16]byte]*series[pointData]{hash: ser}, nil
		},
	}

	_, err := p.queryRatePointsCached(context.Background(), epoch, epoch.Add(5*time.Minute), step, window, 0, timeseries, rateKindRate)
	require.NoError(t, err)

	pts, err := p.queryRatePointsCached(context.Background(), epoch, epoch.Add(10*time.Minute), step, window, 0, timeseries, rateKindRate)
	require.NoError(t, err)
	require.Len(t, pts, 1)

	wantTS := make([]int64, 0, 11)
	for i := range 11 {
		wantTS = append(wantTS, epoch.UnixMilli()+int64(i)*stepMs)
	}
	require.Equal(t, wantTS, pts[0].ts)
}

func labelHash(v uint64) (r [16]byte) {
	binary.LittleEndian.PutUint64(r[:], v)
	return r
}

func makeTestQuerier() *promQuerier {
	return &promQuerier{tracer: nooptrace.NewTracerProvider().Tracer("test")}
}

// makeSeries builds a series with the given label pairs and (ts, val) pairs.
func makeSeries(lbs labels.Labels, points [][2]float64) *series[pointData] {
	s := &series[pointData]{labels: lbs}
	for _, p := range points {
		s.ts = append(s.ts, int64(p[0]))
		s.data.values = append(s.data.values, p[1])
	}
	return s
}

// pointsAt returns the value at timestamp ts from a series, or NaN if absent.
func pointAt(s *series[pointData], ts int64) (float64, bool) {
	for i, t := range s.ts {
		if t == ts {
			return s.data.values[i], true
		}
	}
	return 0, false
}

func TestAggregateSampledPoints(t *testing.T) {
	ctx := context.Background()
	p := makeTestQuerier()

	// Label sets used across sub-tests.
	lbJob1 := labels.FromStrings("__name__", "m", "job", "a", "instance", "1")
	lbJob1b := labels.FromStrings("__name__", "m", "job", "a", "instance", "2") // same job, different instance
	lbJob2 := labels.FromStrings("__name__", "m", "job", "b", "instance", "1")

	h1 := labelHash(lbJob1.Hash())
	h2 := labelHash(lbJob1b.Hash())
	h3 := labelHash(lbJob2.Hash())

	// Three series, two timestamps each.
	//
	//          t=1000  t=2000
	// job=a/1:   2    8
	// job=a/2:   4    6
	// job=b/1:   3    9
	input := map[[16]byte]*series[pointData]{
		h1: makeSeries(lbJob1, [][2]float64{{1000, 2}, {2000, 8}}),
		h2: makeSeries(lbJob1b, [][2]float64{{1000, 4}, {2000, 6}}),
		h3: makeSeries(lbJob2, [][2]float64{{1000, 3}, {2000, 9}}),
	}

	findGroup := func(t *testing.T, result []*series[pointData], match labels.Labels) *series[pointData] {
		t.Helper()
		for _, s := range result {
			if labels.Equal(s.labels, match) {
				return s
			}
		}
		t.Fatalf("group %v not found in result", match)
		return nil
	}

	t.Run("Sum_ByJob", func(t *testing.T) {
		// sum by (job): on=true, groupBy=["job"] — keep only the "job" label.
		sampler, _ := canUseSampledPoints(time.Second, "sum")
		result := p.aggregateSampledPoints(ctx, input, true, []string{"job"}, sampler, time.Second)

		require.Len(t, result, 2)

		gA := findGroup(t, result, labels.FromStrings("job", "a"))
		v, ok := pointAt(gA, 1000)
		require.True(t, ok)
		require.Equal(t, 6.0, v, "sum job=a t=1000: 2+4")

		v, ok = pointAt(gA, 2000)
		require.True(t, ok)
		require.Equal(t, 14.0, v, "sum job=a t=2000: 8+6")

		gB := findGroup(t, result, labels.FromStrings("job", "b"))
		v, ok = pointAt(gB, 1000)
		require.True(t, ok)
		require.Equal(t, 3.0, v)
	})

	t.Run("Min_ByJob", func(t *testing.T) {
		sampler, _ := canUseSampledPoints(time.Second, "min")
		result := p.aggregateSampledPoints(ctx, input, true, []string{"job"}, sampler, time.Second)

		require.Len(t, result, 2)
		gA := findGroup(t, result, labels.FromStrings("job", "a"))

		v, _ := pointAt(gA, 1000)
		require.Equal(t, 2.0, v, "min job=a t=1000: min(2,4)=2")
		v, _ = pointAt(gA, 2000)
		require.Equal(t, 6.0, v, "min job=a t=2000: min(8,6)=6")
	})

	t.Run("Max_ByJob", func(t *testing.T) {
		sampler, _ := canUseSampledPoints(time.Second, "max")
		result := p.aggregateSampledPoints(ctx, input, true, []string{"job"}, sampler, time.Second)

		require.Len(t, result, 2)
		gA := findGroup(t, result, labels.FromStrings("job", "a"))

		v, _ := pointAt(gA, 1000)
		require.Equal(t, 4.0, v, "max job=a t=1000: max(2,4)=4")
		v, _ = pointAt(gA, 2000)
		require.Equal(t, 8.0, v, "max job=a t=2000: max(8,6)=8")
	})

	t.Run("Avg_ByJob", func(t *testing.T) {
		sampler, _ := canUseSampledPoints(time.Second, "avg")
		result := p.aggregateSampledPoints(ctx, input, true, []string{"job"}, sampler, time.Second)

		require.Len(t, result, 2)
		gA := findGroup(t, result, labels.FromStrings("job", "a"))

		v, _ := pointAt(gA, 1000)
		require.Equal(t, 3.0, v, "avg job=a t=1000: (2+4)/2=3")
		v, _ = pointAt(gA, 2000)
		require.Equal(t, 7.0, v, "avg job=a t=2000: (8+6)/2=7")
	})

	t.Run("Sum_Without_Instance", func(t *testing.T) {
		// sum without (instance): on=false, groupBy=["instance"] — drop instance, keep rest.
		// job=a/1 and job=a/2 share {__name__,job} after dropping instance → collapse.
		// job=b/1 is alone in its group.
		sampler, _ := canUseSampledPoints(time.Second, "sum")
		result := p.aggregateSampledPoints(ctx, input, false, []string{"instance"}, sampler, time.Second)

		require.Len(t, result, 2)
	})

	t.Run("OutputSortedByTimestamp", func(t *testing.T) {
		// Feed unsorted timestamps; result must be sorted.
		unordered := map[[16]byte]*series[pointData]{
			h1: makeSeries(lbJob1, [][2]float64{{3000, 1}, {1000, 2}, {2000, 3}}),
		}
		sampler, _ := canUseSampledPoints(time.Second, "sum")
		result := p.aggregateSampledPoints(ctx, unordered, false, []string{"job"}, sampler, time.Second)
		require.Len(t, result, 1)
		require.Equal(t, []int64{1000, 2000, 3000}, result[0].ts)
	})

	t.Run("SingleSeries_SumEqualsValue", func(t *testing.T) {
		// With only one series in the group, all aggregators return the original value.
		single := map[[16]byte]*series[pointData]{
			h3: makeSeries(lbJob2, [][2]float64{{1000, 7}, {2000, 3}}),
		}
		for _, fn := range []string{"sum", "avg", "min", "max"} {
			sampler, _ := canUseSampledPoints(time.Second, fn)
			result := p.aggregateSampledPoints(ctx, single, false, []string{"job"}, sampler, time.Second)
			require.Len(t, result, 1)
			v, _ := pointAt(result[0], 1000)
			require.Equalf(t, 7.0, v, "fn=%s: single-series result must equal original value", fn)
		}
	})
}

// TestCanUseSampledPoints verifies which PromQL function names are accepted by
// canUseSampledPoints and which correctly fall back to the raw-points path.
func TestCanUseSampledPoints(t *testing.T) {
	step := 30 * time.Second

	supported := []string{"", "sum", "sum_over_time", "avg", "avg_over_time", "min", "min_over_time", "max", "max_over_time", "count", "count_over_time"}
	for _, fn := range supported {
		s, ok := canUseSampledPoints(step, fn)
		require.Truef(t, ok, "canUseSampledPoints should accept function %q", fn)
		if fn == "count" {
			// count must set noClientGrouping so PromQL sees all series and counts
			// them itself rather than counting the pre-collapsed single group.
			require.Truef(t, s.noClientGrouping, "count sampler must set noClientGrouping")
		}
	}

	unsupported := []string{"rate", "irate", "delta", "increase", "unknown"}
	for _, fn := range unsupported {
		_, ok := canUseSampledPoints(step, fn)
		require.Falsef(t, ok, "canUseSampledPoints should reject function %q (must fall back to raw points)", fn)
	}

	// Step below 1s must always fall back regardless of function.
	_, ok := canUseSampledPoints(500*time.Millisecond, "sum")
	require.False(t, ok, "step < 1s must always fall back")
}

func TestMetricsCache_Metrics(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))

	mc := newTestCache(t, MetricsCacheOptions{
		MaxBytes:      100 * 1024,
		MeterProvider: meterProvider,
	})

	ctx := context.Background()

	// Perform some operations to generate stats.
	key := metricscache.Key{Hash: [16]byte{1}}
	// Cost will be metricscache.EntryCost(1).
	mc.Update(key, metricscache.Update{
		FetchFrom: 1000,
		UntilMs:   2000,
		TS:        []int64{1000},
		Vals:      []float64{1.0},
	})

	// Hit.
	_, hit := mc.Lookup(key, 1000)
	require.True(t, hit)

	// Miss.
	_, hit = mc.Lookup(metricscache.Key{Hash: [16]byte{2}}, 1000)
	require.False(t, hit)

	// Collect metrics.
	var rm metricdata.ResourceMetrics
	err := reader.Collect(ctx, &rm)
	require.NoError(t, err)

	found := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			found[m.Name] = true
			switch m.Name {
			case "chstorage.metrics_cache.hits":
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(1), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.misses":
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(2), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.size":
				data := m.Data.(metricdata.Gauge[int64])
				require.Equal(t, int64(1), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.capacity_bytes":
				data := m.Data.(metricdata.Gauge[int64])
				require.Equal(t, int64(100*1024), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.disk_size_bytes":
				data := m.Data.(metricdata.Gauge[int64])
				require.Equal(t, int64(0), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.evicted_count":
				// Should be 0 initially.
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(0), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.evicted_cost":
				// Should be 0 initially.
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(0), data.DataPoints[0].Value)
			case "chstorage.metrics_cache.rejected_sets":
				// Should be 0 initially.
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(0), data.DataPoints[0].Value)
			}
		}
	}

	require.True(t, found["chstorage.metrics_cache.hits"])
	require.True(t, found["chstorage.metrics_cache.misses"])
	require.True(t, found["chstorage.metrics_cache.size"])
	require.True(t, found["chstorage.metrics_cache.capacity_bytes"])
	require.True(t, found["chstorage.metrics_cache.disk_size_bytes"])
	require.True(t, found["chstorage.metrics_cache.ratio"])
	require.True(t, found["chstorage.metrics_cache.evicted_count"])
	require.True(t, found["chstorage.metrics_cache.evicted_cost"])
	require.True(t, found["chstorage.metrics_cache.rejected_sets"])
}

func TestQueryPointsCached_GapRegression(t *testing.T) {
	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: 0})

	lb := labels.FromStrings("__name__", "gap_test")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}
	epoch := testEpoch
	stepMs := int64(60_000) // 1 minute per point

	callCount := 0
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		queryPointsFunc: func(_ context.Context, _ string, s, e time.Time, _ map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			callCount++
			ser := &series[pointData]{labels: lb}
			for tstamp := s.UnixMilli(); tstamp <= e.UnixMilli(); tstamp += stepMs {
				ser.ts = append(ser.ts, tstamp)
				ser.data.values = append(ser.data.values, 1.0)
			}
			return map[[16]byte]*series[pointData]{hash: ser}, nil
		},
	}

	ctx := context.Background()

	// 1. Initial query: [epoch - 24h, epoch]
	start1 := epoch.Add(-24 * time.Hour)
	end1 := epoch
	pts, err := p.queryPointsCached(ctx, "points", start1, end1, timeseries)
	require.NoError(t, err)
	require.Len(t, pts[0].ts, 1441) // 24h * 60m + 1
	require.Equal(t, 1, callCount)

	// 2. Disjoint query: User waits 30m, then queries [epoch + 25m, epoch + 30m]
	start2 := epoch.Add(25 * time.Minute)
	end2 := epoch.Add(30 * time.Minute)
	pts, err = p.queryPointsCached(ctx, "points", start2, end2, timeseries)
	require.NoError(t, err)
	require.Len(t, pts[0].ts, 6) // 5 minutes = 6 points
	require.Equal(t, 2, callCount)

	// 3. The gap query: User zooms back out to see [epoch, epoch + 30m]
	// In the buggy code, the cache falsely believes it has everything up to epoch+30m,
	// so it serves a cache hit but completely misses the [epoch+1m, epoch+24m] data.
	start3 := epoch
	end3 := epoch.Add(30 * time.Minute)
	pts, err = p.queryPointsCached(ctx, "points", start3, end3, timeseries)
	require.NoError(t, err)

	// We expect 31 points (epoch to epoch+30m inclusive).
	// Without the fix, this fails because it only returns 7 points (epoch, and epoch+25m to epoch+30m).
	require.Len(t, pts[0].ts, 31)
}

// TestQueryPointsCached_PrependDisjointGap ensures we don't stretch the watermark backwards
// over a gap when querying a disjoint older range.
func TestQueryPointsCached_PrependDisjointGap(t *testing.T) {
	epoch := testEpoch
	lb := labels.FromStrings("__name__", "m_prepend")
	hash := labelHash(lb.Hash())
	timeseries := map[[16]byte]labels.Labels{hash: lb}
	stepMs := int64(60_000)

	mc := newTestCache(t, MetricsCacheOptions{MaxBytes: 1024 * 1024, SafetyLag: time.Minute})
	p := &promQuerier{
		metricsCache: mc,
		tracer:       nooptrace.NewTracerProvider().Tracer("test"),
		queryPointsFunc: func(ctx context.Context, table string, s, e time.Time, ts map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error) {
			ser := &series[pointData]{labels: lb}
			for tstamp := s.UnixMilli(); tstamp <= e.UnixMilli(); tstamp += stepMs {
				ser.ts = append(ser.ts, tstamp)
				ser.data.values = append(ser.data.values, 1.0)
			}
			return map[[16]byte]*series[pointData]{hash: ser}, nil
		},
	}
	ctx := context.Background()

	// 1. Query an initial newer range: [epoch+10m, epoch+15m]
	_, err := p.queryPointsCached(ctx, "points", epoch.Add(10*time.Minute), epoch.Add(15*time.Minute), timeseries)
	require.NoError(t, err)

	// 2. Query a disjoint older range: [epoch, epoch+5m].
	// This will call upsertCache with untilMs = epoch+5m and oldMinTS = epoch+10m.
	// Without untilMs < oldMinTS-1 check, it prepends the data and stretches minTS back to epoch,
	// leaving a gap [epoch+5m, epoch+10m].
	_, err = p.queryPointsCached(ctx, "points", epoch, epoch.Add(5*time.Minute), timeseries)
	require.NoError(t, err)

	// 3. Query the full range: [epoch, epoch+15m].
	// It should be treated as a cache miss, fetching all 16 points.
	pts, err := p.queryPointsCached(ctx, "points", epoch, epoch.Add(15*time.Minute), timeseries)
	require.NoError(t, err)
	require.Len(t, pts, 1)
	require.Len(t, pts[0].ts, 16)
}
