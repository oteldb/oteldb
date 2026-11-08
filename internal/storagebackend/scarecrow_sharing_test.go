package storagebackend

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	nativestorage "github.com/oteldb/storage"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func TestNativeSharingCancellation(t *testing.T) {
	t.Parallel()
	for _, cancelLeader := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "leader"}[cancelLeader], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				group := newNativeQuerySharing(1, 1024)
				t.Cleanup(group.closeCache)
				key := nativeQueryKey{operation: "test"}
				var calls atomic.Int32
				release := make(chan struct{})
				workDone := make(chan struct{})
				compute := func(ctx context.Context) ([]int, error) {
					calls.Add(1)
					defer close(workDone)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-release:
						return []int{42}, nil
					}
				}
				leader, stopLeader := context.WithCancel(fetch.WithScope(t.Context(), fetch.NewScope()))
				follower, stopFollower := context.WithCancel(fetch.WithScope(t.Context(), fetch.NewScope()))
				defer stopLeader()
				defer stopFollower()
				results := make([][]int, 2)
				errs := make([]error, 2)
				go func() {
					results[0], errs[0] = shareNative(leader, group, key, compute, func([]int) int64 { return 8 }, slices.Clone)
				}()
				synctest.Wait()
				go func() {
					results[1], errs[1] = shareNative(follower, group, key, compute, func([]int) int64 { return 8 }, slices.Clone)
				}()
				synctest.Wait()
				canceled := 1
				if cancelLeader {
					stopLeader()
					canceled = 0
				} else {
					stopFollower()
				}
				synctest.Wait()
				require.ErrorIs(t, errs[canceled], context.Canceled)
				require.Equal(t, int32(1), calls.Load())
				close(release)
				synctest.Wait()
				require.NoError(t, errs[1-canceled])
				require.Equal(t, []int{42}, results[1-canceled])
				<-workDone
				require.Empty(t, group.calls)
				require.Zero(t, group.active)
			})
		})
	}
}

func TestNativeSharingLastWaiterCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(1, 1024)
		t.Cleanup(group.closeCache)
		ctx, cancel := context.WithCancel(t.Context())
		finished := make(chan struct{})
		go func() {
			_, err := shareNative(ctx, group, nativeQueryKey{}, func(ctx context.Context) (int, error) {
				defer close(finished)
				<-ctx.Done()
				return 0, ctx.Err()
			}, func(int) int64 { return 8 }, func(v int) int { return v })
			require.ErrorIs(t, err, context.Canceled)
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		<-finished
		require.Empty(t, group.calls)
		require.Zero(t, group.active)
	})
}

func TestNativeSharingLimitsAndOwnership(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		operations int
		bytes      int64
		want       int32
	}{
		{"disabled-operations", 0, 1024, 6},
		{"disabled-bytes", 6, 0, 6},
		{"oversized", 6, 1, 6},
		{"shared", 6, 1024, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				group := newNativeQuerySharing(tt.operations, tt.bytes)
				t.Cleanup(group.closeCache)
				release := make(chan struct{})
				var calls atomic.Int32
				compute := func(context.Context) ([]int, error) { calls.Add(1); <-release; return []int{42}, t.Context().Err() }
				var results [6][]int
				for i := range results {
					go func() {
						var err error
						results[i], err = shareNative(t.Context(), group, nativeQueryKey{}, compute, func([]int) int64 { return 8 }, slices.Clone)
						require.NoError(t, err)
					}()
				}
				synctest.Wait()
				close(release)
				synctest.Wait()
				require.Equal(t, tt.want, calls.Load())
				results[0][0] = -1
				for _, got := range results[1:] {
					require.Equal(t, []int{42}, got)
				}
				_, err := shareNative(t.Context(), group, nativeQueryKey{}, compute, func([]int) int64 { return 8 }, slices.Clone)
				require.NoError(t, err)
				require.Equal(t, tt.want+1, calls.Load(), "a completed result must never hide an acknowledged write")
			})
		})
	}
}

func TestNativeSharingSaturation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(1, 1024)
		t.Cleanup(group.closeCache)
		ctx, cancel := context.WithCancel(t.Context())
		exit := make(chan struct{})
		go func() {
			_, _ = shareNative(ctx, group, nativeQueryKey{}, func(context.Context) (int, error) { <-exit; return 1, nil }, func(int) int64 { return 8 }, func(n int) int { return n })
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.Equal(t, 1, group.active, "canceled work still occupies admission until it exits")
		// Saturation must run inline, never queue behind an uncooperative producer.
		n, err := shareNative(t.Context(), group, nativeQueryKey{}, func(context.Context) (int, error) { return 2, nil }, func(int) int64 { return 8 }, func(n int) int { return n })
		require.NoError(t, err)
		require.Equal(t, 2, n)
		close(exit)
		synctest.Wait()
		require.Zero(t, group.active)
	})
}

func TestNativeSharingKey(t *testing.T) {
	t.Parallel()
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "instance", "a"), labels.MustNewMatcher(labels.MatchRegexp, "mode", "idle|user")}
	grid := scarecrow.WindowGrid{Start: 100, Step: 15, Width: 60, NumSteps: 10}
	key := nativeKey("grid", grid, "cpu", ms)
	reversed := []*labels.Matcher{ms[1], ms[0]}
	require.Equal(t, key, nativeKey("grid", grid, "cpu", reversed))
	for _, other := range []nativeQueryKey{
		nativeKey("count", grid, "cpu", ms), nativeKey("grid", grid, "mode", ms),
		nativeKey("grid", grid, "cpu", ms[:1]),
		nativeKey("grid", scarecrow.WindowGrid{Start: 101, Step: 15, Width: 60, NumSteps: 10}, "cpu", ms),
		nativeKey("grid", scarecrow.WindowGrid{Start: 100, Step: 16, Width: 60, NumSteps: 10}, "cpu", ms),
		nativeKey("grid", scarecrow.WindowGrid{Start: 100, Step: 15, Width: 61, NumSteps: 10}, "cpu", ms),
		nativeKey("grid", scarecrow.WindowGrid{Start: 100, Step: 15, Width: 60, NumSteps: 11}, "cpu", ms),
	} {
		require.NotEqual(t, key, other)
	}
}

type sharedNativeSource struct {
	Source
	fetch.Fetcher
	release    <-chan struct{}
	counts     atomic.Int32
	aggregates atomic.Int32
}

func (s *sharedNativeSource) WithQueryBudget(ctx context.Context) context.Context { return ctx }

func (s *sharedNativeSource) CountGrid(ctx context.Context, _ fetch.Request, grid fetch.WindowGrid, _ []byte) (map[string][]int, error) {
	s.counts.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
	}
	out := map[string][]int{"0": make([]int, grid.NumSteps), "1": make([]int, grid.NumSteps)}
	for _, values := range out {
		for i := range values {
			values[i] = 1
		}
	}
	return out, nil
}

func (s *sharedNativeSource) AggregateMetricsWindowNamed(_ context.Context, _ signal.TenantID, req fetch.Request, spec engine.WindowSpec) ([]engine.NamedWindowAgg, error) {
	s.aggregates.Add(1)
	var out []engine.NamedWindowAgg
	for mode := range 6 {
		series := signal.Series{Attributes: signal.Attributes{{Key: []byte("__name__"), Value: signal.StringValue([]byte("cpu_seconds"))}, {Key: []byte("mode"), Value: signal.StringValue([]byte(string(rune('a' + mode))))}}}
		match := true
		for _, m := range req.Matchers {
			if string(m.Name) == "mode" && !m.Match(series.Attributes[1].Value) {
				match = false
			}
		}
		if !match {
			continue
		}
		windows := make([]engine.WindowAgg, 0)
		for end := spec.Anchor; end <= req.End; end += spec.Step {
			windows = append(windows, engine.WindowAgg{End: end, Rows: 1, Count: 1, Sum: 4})
		}
		out = append(out, engine.NamedWindowAgg{Series: series, Windows: windows})
	}
	return out, nil
}

type sharedTestSource struct{ *sharedNativeSource }

func (s sharedTestSource) Fetcher(...signal.TenantID) fetch.Fetcher { return s.sharedNativeSource }

func TestNativeSharingSixModeDenominator(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		source := &sharedNativeSource{release: release}
		backend := NewQuery(sharedTestSource{source}, WithNativeQuerySharing(64, 1<<20))
		t.Cleanup(backend.CloseQueryCache)
		queryEngine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
		var wg sync.WaitGroup
		for mode := range 6 {
			wg.Go(func() {
				query := `sum(sum_over_time(cpu_seconds{mode="` + string(rune('a'+mode)) + `"}[1m])) / scalar(count(count(cpu_seconds) by(cpu)))`
				q, err := queryEngine.NewRangeQuery(t.Context(), backend, nil, query, time.Unix(100, 0), time.Unix(160, 0), 15*time.Second)
				require.NoError(t, err)
				defer q.Close()
				result := q.Exec(t.Context())
				require.NoError(t, result.Err)
				require.Contains(t, result.Value.String(), "2 @")
			})
		}
		synctest.Wait()
		require.Equal(t, int32(1), source.counts.Load(), "six independent queries share their count grid")
		close(release)
		wg.Wait()
		require.Equal(t, int32(6), source.aggregates.Load(), "mode-specific numerators remain distinct")
	})
}

func TestNativeSharingScopeAndBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(1, 1024)
		t.Cleanup(group.closeCache)
		scope := fetch.NewScope()
		budget := readbudget.New(100)
		ctx := readbudget.With(fetch.WithScope(t.Context(), scope), budget)
		_, err := shareNative(ctx, group, nativeQueryKey{}, func(ctx context.Context) (int, error) {
			require.Same(t, scope, fetch.ScopeFrom(ctx))
			require.Same(t, budget, readbudget.From(ctx))
			return 1, nil
		}, func(int) int64 { return 8 }, func(n int) int { return n })
		require.NoError(t, err)
		require.Empty(t, group.calls)
		require.Zero(t, group.active)
	})
}

func TestNativeSharingErrorsAreNotCached(t *testing.T) {
	t.Parallel()
	errExpected := errors.New("read failed")
	group := newNativeQuerySharing(1, 1024)
	t.Cleanup(group.closeCache)
	for range 2 {
		_, err := shareNative(t.Context(), group, nativeQueryKey{}, func(context.Context) (int, error) { return 0, errExpected }, func(int) int64 { return 8 }, func(n int) int { return n })
		require.ErrorIs(t, err, errExpected)
	}
}

func TestNativeSharingSourceIsolation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		source := &sharedNativeSource{release: release}
		readSource := sharedTestSource{source}
		require.Nil(t, NewQuery(readSource).sharing, "context-authorized custom sources must opt in")
		first := NewQuery(readSource, WithNativeQuerySharing(64, 1024))
		second := NewQuery(readSource, WithNativeQuerySharing(64, 1024))
		first.tenant, second.tenant = "tenant-a", "tenant-b"
		grid := scarecrow.WindowGrid{Start: 100_000, Step: 15_000, Width: 60_000, NumSteps: 3}
		var wg sync.WaitGroup
		for _, backend := range []*Backend{first, second} {
			wg.Go(func() {
				counts, err := backend.ScarecrowScanner().(scarecrow.GridSeriesCounter).CountSeriesGrid(t.Context(), grid, "cpu", nil)
				require.NoError(t, err)
				require.Len(t, counts, 2)
			})
		}
		synctest.Wait()
		require.Equal(t, int32(2), source.counts.Load(), "a flight is never reused across backend/tenant identities")
		close(release)
		wg.Wait()
	})
}

func TestNativeSharingCompletedCache(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                    string
		cacheBytes, resultBytes int64
		want                    int
	}{
		{"enabled", 4096, 4096, 1},
		{"disabled", 0, 4096, 2},
		{"oversized-result", 4096, 1, 2},
		{"oversized-cache", 1, 4096, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			group := newNativeQuerySharing(8, tt.resultBytes)
			t.Cleanup(group.closeCache)
			group.maxCacheBytes = tt.cacheBytes
			key := nativeQueryKey{operation: "cache", cacheable: true}
			calls := 0
			compute := func(ctx context.Context) ([]int, error) { calls++; return []int{42}, ctx.Err() }
			first, err := shareNative(t.Context(), group, key, compute, func([]int) int64 { return 8 }, slices.Clone)
			require.NoError(t, err)
			first[0] = -1
			second, err := shareNative(t.Context(), group, key, compute, func([]int) int64 { return 8 }, slices.Clone)
			require.NoError(t, err)
			require.Equal(t, []int{42}, second)
			require.Equal(t, tt.want, calls)
			if group.cache != nil {
				require.LessOrEqual(t, int64(group.cache.Capacity()), group.maxCacheBytes)
			}
			require.Empty(t, group.calls, "completed cache retains values, never request contexts or budgets")
		})
	}
}

func TestNativeSharingCacheEviction(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 4096)
		t.Cleanup(group.closeCache)
		group.maxCacheBytes = 4096
		calls := 0
		read := func(start int64) {
			_, err := shareNative(t.Context(), group, nativeQueryKey{cacheable: true, grid: scarecrow.WindowGrid{Start: start}}, func(ctx context.Context) (int, error) { calls++; return 42, ctx.Err() }, func(int) int64 { return 8 }, func(n int) int { return n })
			require.NoError(t, err)
		}
		read(1)
		read(1)
		require.Equal(t, 1, calls)
		for i := range 64 {
			read(int64(i + 2))
			synctest.Wait()
		}
		var bytes int64
		group.cache.Range(func(_ nativeQueryKey, e *nativeCacheEntry) bool { bytes += e.bytes; return true })
		require.LessOrEqual(t, bytes, group.maxCacheBytes)
		require.Less(t, group.cache.Size(), 33)
		before := calls
		read(1)
		require.Equal(t, before+1, calls, "the old completed result was evicted")
	})
}

type cachedTestSource struct {
	sharedTestSource
	revision nativestorage.MetricReadRevision
	now      int64
}

func (s *cachedTestSource) MetricReadRevision(_ signal.TenantID, _ int64) (nativestorage.MetricReadRevision, bool) {
	return s.revision, s.revision.ValidUntil == 0 || s.now < s.revision.ValidUntil
}

func TestNativeSharingAgeDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		close(release)
		delegate := &sharedNativeSource{release: release}
		now := time.Now().UnixNano()
		source := &cachedTestSource{sharedTestSource: sharedTestSource{delegate}, revision: nativestorage.MetricReadRevision{ValidUntil: now + 2}, now: now}
		backend := NewQuery(source, WithNativeQuerySharing(8, 4096))
		t.Cleanup(backend.CloseQueryCache)
		grid := scarecrow.WindowGrid{Start: 100_000, Step: 15_000, Width: 60_000, NumSteps: 2}
		read := func() {
			_, err := backend.ScarecrowScanner().(scarecrow.GridSeriesCounter).CountSeriesGrid(t.Context(), grid, "cpu", nil)
			require.NoError(t, err)
		}
		read()
		read()
		require.Equal(t, int32(1), delegate.counts.Load())
		time.Sleep(2 * time.Nanosecond)
		read()
		read()
		require.Equal(t, int32(3), delegate.counts.Load(), "even a source reporting cacheable cannot extend the exact age deadline")
		source.now = time.Now().UnixNano()
		source.revision.ValidUntil = now + 100
		read()
		require.Equal(t, int32(4), delegate.counts.Load(), "a policy age change produces a separate cache key")
	})
}

type cacheCountSource struct {
	*nativestorage.Storage
	calls atomic.Int32
}

func (s *cacheCountSource) Fetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return cacheCountFetcher{Fetcher: s.Storage.Fetcher(tenants...), calls: &s.calls}
}

type cacheCountFetcher struct {
	fetch.Fetcher
	calls *atomic.Int32
}

func (f cacheCountFetcher) CountGrid(ctx context.Context, request fetch.Request, grid fetch.WindowGrid, label []byte) (map[string][]int, error) {
	f.calls.Add(1)
	return fetch.GridCounterOf(f.Fetcher).CountGrid(ctx, request, grid, label)
}

func TestNativeSharingAcknowledgedWrite(t *testing.T) {
	t.Parallel()
	store, err := nativestorage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
	source := &cacheCountSource{Storage: store}
	backend := NewQuery(source, WithNativeQuerySharing(8, 4096))
	t.Cleanup(backend.CloseQueryCache)
	write := func(cpu string) {
		var md metric.Metrics
		m := md.AddResource().AddScope().AddMetric()
		m.Name, m.Kind = []byte("counter"), metric.KindGauge
		p := m.AddPoint()
		p.Ts, p.Value = 100_000*int64(time.Millisecond), 1
		p.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(cpu))})
		_, err := store.WriteMetrics(t.Context(), md)
		require.NoError(t, err)
	}
	grid := scarecrow.WindowGrid{Start: 100_000, Step: 15_000, Width: 60_000, NumSteps: 2}
	read := func() map[string][]uint64 {
		values, err := backend.ScarecrowScanner().(scarecrow.GridSeriesCounter).CountSeriesGrid(t.Context(), grid, "cpu", nil)
		require.NoError(t, err)
		return values
	}
	write("0")
	first := read()
	first["0"][0] = 99
	require.Equal(t, uint64(1), read()["0"][0], "callers own their output copies")
	require.Equal(t, int32(1), source.calls.Load())
	write("1")
	require.Len(t, read(), 2, "a new request observes the acknowledged write")
	require.Equal(t, int32(2), source.calls.Load())
	require.Len(t, read(), 2)
	require.Equal(t, int32(2), source.calls.Load())
	require.NoError(t, store.Reset(t.Context()))
	require.Empty(t, read(), "reset never reuses the old engine revision")
	require.Equal(t, int32(3), source.calls.Load())
}

func TestNativeSharingCommonFold(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	close(release)
	delegate := &sharedNativeSource{release: release}
	source := &cachedTestSource{sharedTestSource: sharedTestSource{delegate}}
	backend := NewQuery(source, WithNativeQuerySharing(8, 1<<20))
	t.Cleanup(backend.CloseQueryCache)
	queryEngine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
	for _, function := range []string{"sum", "avg", "min", "max", "stddev", "stdvar"} {
		q, err := queryEngine.NewRangeQuery(t.Context(), backend, nil, function+`(avg_over_time(cpu_seconds[1m]))`, time.Unix(100, 0), time.Unix(160, 0), 15*time.Second)
		require.NoError(t, err)
		result := q.Exec(t.Context())
		q.Close()
		require.NoError(t, result.Err)
		require.NotEmpty(t, result.Value.String())
	}
	require.Equal(t, int32(1), delegate.aggregates.Load(), "six different queries reuse the same native fold")
	grid := scarecrow.WindowGrid{Start: 100_000, Step: 15_000, Width: 60_000, NumSteps: 5}
	first, err := backend.ScarecrowScanner().(scarecrow.GridAggregateScanner).AggregateGrid(t.Context(), grid, nil)
	require.NoError(t, err)
	first[0].Windows[0].Sum = -1
	first[0].Labels = labels.FromStrings("modified", "true")
	second, err := backend.ScarecrowScanner().(scarecrow.GridAggregateScanner).AggregateGrid(t.Context(), grid, nil)
	require.NoError(t, err)
	require.Equal(t, float64(4), second[0].Windows[0].Sum)
	require.Empty(t, second[0].Labels.Get("modified"))
}

func TestNativeSharingOverlappingWrite(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 4096)
		t.Cleanup(group.closeCache)
		group.maxCacheBytes = 4096
		eng := engine.New(engine.Config{})
		key := func() nativeQueryKey {
			revision, ok := eng.ReadRevision()
			require.True(t, ok)
			return nativeQueryKey{cacheable: true, revision: nativestorage.MetricReadRevision{Revision: revision}}
		}
		oldKey := key()
		release := make(chan struct{})
		var calls atomic.Int32
		compute := func(ctx context.Context) (int32, error) {
			snapshot := calls.Add(1)
			<-release
			return snapshot, ctx.Err()
		}
		read := func(key nativeQueryKey) int32 {
			value, err := shareNative(t.Context(), group, key, compute, func(int32) int64 { return 8 }, func(n int32) int32 { return n })
			require.NoError(t, err)
			return value
		}
		var first, second int32
		go func() { first = read(oldKey) }()
		synctest.Wait()
		_, err := eng.Append(signal.Series{}, 1, 1)
		require.NoError(t, err)
		newKey := key()
		require.NotEqual(t, oldKey, newKey)
		go func() { second = read(newKey) }()
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load(), "overlapping requests share the first snapshot even across an acknowledged write")
		close(release)
		synctest.Wait()
		require.Equal(t, int32(1), first)
		require.Equal(t, first, second)
		require.Equal(t, int32(2), read(newKey), "completed results are never retagged with a later revision")
		require.Equal(t, int32(2), read(newKey))
		require.Equal(t, int32(2), calls.Load())
	})
}
