package storagebackend

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	nativestorage "github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func writeSharingCounter(t *testing.T, store *nativestorage.Storage, cpu string) {
	t.Helper()
	var md metric.Metrics
	m := md.AddResource().AddScope().AddMetric()
	m.Name, m.Kind = []byte("counter"), metric.KindGauge
	for i, ts := range []int64{100_000, 115_000} {
		point := m.AddPoint()
		point.Ts, point.Value = ts*int64(time.Millisecond), float64(i*15)
		point.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(cpu))})
	}
	_, err := store.WriteMetrics(t.Context(), md)
	require.NoError(t, err)
}

func TestNativeSharingHeldRawAdmission(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"head", "file-parts"} {
		t.Run(state, func(t *testing.T) {
			var store *nativestorage.Storage
			var err error
			if state == "head" {
				store, err = nativestorage.InMemory(nativestorage.WithDecodeMemory(1))
			} else {
				dir := t.TempDir()
				files, openErr := backendfile.New(filepath.Join(dir, "parts"))
				require.NoError(t, openErr)
				store, err = nativestorage.Open(t.Context(), nativestorage.Options{}, nativestorage.WithBackend(files),
					nativestorage.WithWALDir(filepath.Join(dir, "wal")), nativestorage.WithFlushInterval(-1), nativestorage.WithDecodeMemory(1))
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
			writeSharingCounter(t, store, "0")
			if state == "file-parts" {
				require.NoError(t, store.Admin().Flush(t.Context(), "", signal.Metric))
			}
			backend := New(store, WithNativeQueryCache(0))
			t.Cleanup(backend.CloseQueryCache)
			synctest.Test(t, func(t *testing.T) {
				leader := backend.ScarecrowScanner().(*scarecrowScanner)
				follower := backend.ScarecrowScanner().(*scarecrowScanner)
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter")}
				raw, err := follower.Scan(t.Context(), 90_000, 120_000, matchers)
				require.NoError(t, err)
				defer func() { require.NoError(t, raw.Close()) }()
				samples, err := raw.Next(t.Context())
				require.NoError(t, err)
				require.NotNil(t, samples)
				holding, inspected := follower.scope.TryEnter()
				require.True(t, inspected)
				follower.scope.Abort()
				grid := scarecrow.WindowGrid{Start: 115_000, Step: 15_000, Width: 60_000, NumSteps: 1}
				if !holding {
					require.Equal(t, "head", state, "file raw reads must own decode admission")
					counts, err := follower.CountSeriesGrid(t.Context(), grid, "cpu", matchers)
					require.NoError(t, err)
					require.Equal(t, []uint64{1}, counts["0"])
					return
				}
				leaderDone := make(chan struct{})
				go func() {
					defer close(leaderDone)
					_, err := leader.CountSeriesGrid(t.Context(), grid, "cpu", matchers)
					require.NoError(t, err)
				}()
				synctest.Wait()
				_, inspected = leader.scope.TryEnter()
				if inspected {
					leader.scope.Abort()
				}
				require.False(t, inspected, "the leader is queued for the quota held by the follower")
				before := time.Now()
				counts, err := follower.CountSeriesGrid(t.Context(), grid, "cpu", matchers)
				require.NoError(t, err)
				require.Equal(t, []uint64{1}, counts["0"])
				require.Equal(t, before, time.Now(), "the follower must not wait for forced admission or a timeout")
				require.NoError(t, raw.Close())
				synctest.Wait()
				<-leaderDone
			})
		})
	}
}

func TestNativeSharingJoinExcludesConcurrentAdmission(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 4096)
		t.Cleanup(group.closeCache)
		leaderScope, followerScope := fetch.NewScope(), fetch.NewScope()
		release := make(chan struct{})
		compute := func(ctx context.Context) (int, error) { <-release; return 42, ctx.Err() }
		var wg sync.WaitGroup
		read := func(scope *fetch.Scope) {
			wg.Go(func() {
				_, err := shareNative(fetch.WithScope(t.Context(), scope), group, nativeQueryKey{}, compute, func(int) int64 { return 8 }, func(n int) int { return n })
				require.NoError(t, err)
			})
		}
		read(leaderScope)
		synctest.Wait()
		read(followerScope)
		synctest.Wait()
		acquired := make(chan struct{})
		started := make(chan struct{})
		go func() {
			close(started)
			followerScope.Enter()
			followerScope.Charge(1)
			close(acquired)
		}()
		<-started
		_, inspected := followerScope.TryEnter()
		if inspected {
			followerScope.Abort()
		}
		require.False(t, inspected, "the follower's empty admission scope stays guarded during the join")
		select {
		case <-acquired:
			t.Fatal("another branch acquired the follower's first reservation during its shared wait")
		default:
		}
		close(release)
		wg.Wait()
		synctest.Wait()
		<-acquired
		followerScope.Release(1)
	})
}

func TestNativeSharingSameScopeJoin(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 4096)
		t.Cleanup(group.closeCache)
		scope := fetch.NewScope()
		ctx := fetch.WithScope(t.Context(), scope)
		release := make(chan struct{})
		var calls atomic.Int32
		compute := func(ctx context.Context) (int, error) {
			calls.Add(1)
			<-release
			scope.Enter()
			scope.Charge(1)
			scope.Release(1)
			return 42, ctx.Err()
		}
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				_, err := shareNative(ctx, group, nativeQueryKey{}, compute, func(int) int64 { return 8 }, func(n int) int { return n })
				require.NoError(t, err)
			})
			synctest.Wait()
		}
		close(release)
		wg.Wait()
		require.Equal(t, int32(1), calls.Load())
	})
}

func TestNativeSharingOversizedFollowerReleasesScope(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 1)
		t.Cleanup(group.closeCache)
		leaderScope, followerScope := fetch.NewScope(), fetch.NewScope()
		release := make(chan struct{})
		var calls atomic.Int32
		compute := func(ctx context.Context) (int, error) {
			calls.Add(1)
			<-release
			scope := fetch.ScopeFrom(ctx)
			scope.Enter()
			scope.Charge(1)
			scope.Release(1)
			return 42, ctx.Err()
		}
		var wg sync.WaitGroup
		for _, scope := range []*fetch.Scope{leaderScope, followerScope} {
			wg.Go(func() {
				_, err := shareNative(fetch.WithScope(t.Context(), scope), group, nativeQueryKey{}, compute, func(int) int64 { return 8 }, func(n int) int { return n })
				require.NoError(t, err)
			})
			synctest.Wait()
		}
		close(release)
		wg.Wait()
		require.Equal(t, int32(2), calls.Load(), "oversized fallback must release its join guard before computing under its own scope")
	})
}

type metadataSnapshotSource struct {
	*nativestorage.Storage
	calls   atomic.Int32
	release <-chan struct{}
}

func (s *metadataSnapshotSource) MetricSeries(ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64) ([]signal.Series, error) {
	series, err := s.Storage.MetricSeries(ctx, tenant, matchers, start, end)
	if s.calls.Add(1) == 1 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	return series, err
}

func TestNativeSharingMetadataWrite(t *testing.T) {
	t.Parallel()
	store, err := nativestorage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
	writeSharingCounter(t, store, "0")
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		source := &metadataSnapshotSource{Storage: store, release: release}
		backend := NewQuery(source, WithNativeQuerySharing(8, 4096))
		t.Cleanup(backend.CloseQueryCache)
		oldDone := make(chan struct{})
		go func() {
			defer close(oldDone)
			old, err := backend.ScarecrowScanner().Series(t.Context(), 55_000, 115_000, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter")})
			require.NoError(t, err)
			require.Len(t, old, 1)
		}()
		synctest.Wait()
		writeSharingCounter(t, store, "1")
		queryEngine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
		freshDone := make(chan struct{})
		go func() {
			defer close(freshDone)
			q, err := queryEngine.NewInstantQuery(t.Context(), backend, nil, `sum(irate(counter[1m]))`, time.UnixMilli(115_000))
			require.NoError(t, err)
			result := q.Exec(t.Context())
			q.Close()
			require.NoError(t, result.Err)
			require.Contains(t, result.Value.String(), "2 @")
		}()
		synctest.Wait()
		require.Equal(t, int32(2), source.calls.Load(), "metadata after the acknowledged write cannot join the older schema")
		select {
		case <-freshDone:
		default:
			t.Fatal("the fresh irate query joined stale metadata")
		}
		close(release)
		<-oldDone
	})
}

func TestNativeSharingProducerScopeCycle(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		group := newNativeQuerySharing(8, 4096)
		t.Cleanup(group.closeCache)
		scopeA, scopeB := fetch.NewScope(), fetch.NewScope()
		gate := make(chan struct{})
		defer func() { cancel(); close(gate) }()
		var callsX, callsY atomic.Int32
		compute := func(calls *atomic.Int32) func(context.Context) (int, error) {
			return func(ctx context.Context) (int, error) {
				if calls.Add(1) == 1 {
					<-gate
				}
				scope := fetch.ScopeFrom(ctx)
				scope.Enter()
				scope.Charge(1)
				scope.Release(1)
				return 42, ctx.Err()
			}
		}
		read := func(scope *fetch.Scope, operation string, fn func(context.Context) (int, error)) {
			_, _ = shareNative(fetch.WithScope(ctx, scope), group, nativeQueryKey{operation: operation}, fn, func(int) int64 { return 8 }, func(n int) int { return n })
		}
		x, y := compute(&callsX), compute(&callsY)
		go read(scopeA, "x", x)
		synctest.Wait()
		go read(scopeB, "y", y)
		synctest.Wait()
		crossDone := make(chan struct{}, 2)
		go func() { read(scopeA, "y", y); crossDone <- struct{}{} }()
		go func() { read(scopeB, "x", x); crossDone <- struct{}{} }()
		synctest.Wait()
		require.Len(t, crossDone, 2, "a scope owning an active producer must never be guarded while joining another producer")
		require.Equal(t, int32(2), callsX.Load())
		require.Equal(t, int32(2), callsY.Load())
	})
}

type (
	observedScope struct {
		request, ctx *fetch.Scope
		operation    string
	}
	scopeCaptureSource struct {
		Source
		seen chan observedScope
	}
)

func (s scopeCaptureSource) Fetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return scopeCaptureFetcher{Fetcher: s.Source.Fetcher(tenants...), seen: s.seen}
}

type scopeCaptureFetcher struct {
	fetch.Fetcher
	seen chan<- observedScope
}

func (f scopeCaptureFetcher) CountGrid(ctx context.Context, request fetch.Request, grid fetch.WindowGrid, label []byte) (map[string][]int, error) {
	f.seen <- observedScope{request: request.Scope, ctx: fetch.ScopeFrom(ctx), operation: "count"}
	return fetch.GridCounterOf(f.Fetcher).CountGrid(ctx, request, grid, label)
}

func (f scopeCaptureFetcher) Fetch(ctx context.Context, request fetch.Request) (fetch.Iterator, error) {
	f.seen <- observedScope{request: request.Scope, ctx: fetch.ScopeFrom(ctx), operation: "raw"}
	return f.Fetcher.Fetch(ctx, request)
}

func TestNativeSharingPrivateScopeOwnership(t *testing.T) {
	t.Parallel()
	store, err := nativestorage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.WithoutCancel(t.Context()))) })
	writeSharingCounter(t, store, "0")
	source := scopeCaptureSource{Source: store, seen: make(chan observedScope, 4)}
	first := NewQuery(source, WithNativeQuerySharing(8, 4096)).ScarecrowScanner()
	second := NewQuery(source, WithNativeQuerySharing(8, 4096)).ScarecrowScanner()
	callerScope := fetch.NewScope()
	ctx := fetch.WithScope(t.Context(), callerScope)
	grid := scarecrow.WindowGrid{Start: 115_000, Step: 15_000, Width: 60_000, NumSteps: 1}
	var private [2]*fetch.Scope
	for i, scanner := range []scarecrow.Scanner{first, second} {
		_, err := scanner.(scarecrow.GridSeriesCounter).CountSeriesGrid(ctx, grid, "cpu", nil)
		require.NoError(t, err)
		observed := <-source.seen
		require.Equal(t, "count", observed.operation)
		require.NotNil(t, observed.request)
		require.Same(t, observed.request, observed.ctx)
		require.NotSame(t, callerScope, observed.request)
		private[i] = observed.request
		iterator, err := scanner.Scan(ctx, 90_000, 120_000, nil)
		require.NoError(t, err)
		observed = <-source.seen
		require.Equal(t, "raw", observed.operation)
		require.Same(t, private[i], observed.request, "raw reads use that scanner's same private admission scope")
		require.NoError(t, iterator.Close())
	}
	require.NotSame(t, private[0], private[1], "reusing a caller context cannot share a scope across Backend groups")
}
