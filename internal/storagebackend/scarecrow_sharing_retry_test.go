package storagebackend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

func TestNativeSharingHealthyCallerRetriesContextError(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				group := newNativeQuerySharing(8, 4096)
				t.Cleanup(group.closeCache)
				release := make(chan struct{})
				var calls atomic.Int32
				compute := func(ctx context.Context) (int, error) {
					if calls.Add(1) == 1 {
						<-release
						return 0, cause
					}
					scope := fetch.ScopeFrom(ctx)
					scope.Enter()
					scope.Charge(1)
					scope.Release(1)
					return 42, ctx.Err()
				}
				var wg sync.WaitGroup
				for range 2 {
					ctx := fetch.WithScope(t.Context(), fetch.NewScope())
					wg.Go(func() {
						value, err := shareNative(ctx, group, nativeQueryKey{}, compute, func(int) int64 { return 8 }, func(n int) int { return n })
						require.NoError(t, err)
						require.Equal(t, 42, value)
					})
					synctest.Wait()
				}
				close(release)
				wg.Wait()
				require.Equal(t, int32(3), calls.Load(), "each healthy caller retries with its own scope after releasing any follower guard")
			})
		})
	}
}

func TestNativeSharingContextRetryIsBounded(t *testing.T) {
	t.Parallel()
	group := newNativeQuerySharing(8, 4096)
	t.Cleanup(group.closeCache)
	calls := 0
	_, err := shareNative(t.Context(), group, nativeQueryKey{}, func(context.Context) (int, error) { calls++; return 0, context.Canceled }, func(int) int64 { return 8 }, func(n int) int { return n })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, calls)
}

func TestNativeSharingCloseDuringProducer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		group := newNativeQuerySharing(8, 4096)
		group.maxCacheBytes = 64 << 10
		backend := &Backend{sharing: group}
		release := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			value, err := shareNative(t.Context(), group, nativeQueryKey{cacheable: true}, func(ctx context.Context) (int, error) { <-release; return 42, ctx.Err() }, func(int) int64 { return 8 }, func(n int) int { return n })
			require.NoError(t, err)
			require.Equal(t, 42, value)
		}()
		synctest.Wait()
		backend.CloseQueryCache()
		backend.CloseQueryCache()
		close(release)
		<-done
		require.True(t, group.closed)
		require.Nil(t, group.cache, "a late producer cannot recreate or write the closed cache")
	})
}

func TestNativeSharingCloseDisabledCache(t *testing.T) {
	t.Parallel()
	backend := NewQuery(nil, WithNativeQuerySharing(0, 0))
	require.NotPanics(t, func() { backend.CloseQueryCache(); backend.CloseQueryCache() })
}
