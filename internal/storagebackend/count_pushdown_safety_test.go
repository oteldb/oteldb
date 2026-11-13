package storagebackend

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func TestCountPushdownSafety(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var safety countPushdownSafety
		safe, release := safety.lease()
		require.True(t, safe)
		require.True(t, safety.valid())
		published := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := safety.invalidate(t.Context()); err == nil {
				close(published)
			}
		})
		synctest.Wait()
		select {
		case <-published:
			t.Fatal("incompatible publication bypassed query lease")
		default:
		}
		release()
		wg.Wait()
		require.False(t, safety.valid())
		safe, release = safety.lease()
		require.False(t, safe)
		require.Nil(t, release)
		require.NoError(t, safety.invalidate(t.Context()))
		require.False(t, safety.valid())
	})
}

func TestSampleOnlyScannerHidesPushdowns(t *testing.T) {
	t.Parallel()
	base := &scarecrowScanner{}
	scanner := sampleOnlyScarecrowScanner{Scanner: base, budget: base}
	_, ok := any(scanner).(scarecrow.SeriesCounter)
	require.False(t, ok)
	_, ok = any(scanner).(scarecrow.GroupedSeriesCounter)
	require.False(t, ok)
	_, ok = any(scanner).(scarecrow.GridSeriesCounter)
	require.False(t, ok)
	_, ok = any(scanner).(scarecrow.AggregateScanner)
	require.False(t, ok)
	_, ok = any(scanner).(scarecrow.GridAggregateScanner)
	require.False(t, ok)
	var closed int
	base.release = sync.OnceFunc(func() { closed++ })
	require.NoError(t, scanner.Close())
	require.NoError(t, scanner.Close())
	require.Equal(t, 1, closed)
}

func TestCountPushdownInvalidationCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var safety countPushdownSafety
		safe, release := safety.lease()
		require.True(t, safe)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- safety.invalidate(ctx) }()
		synctest.Wait()
		safe, later := safety.lease()
		require.False(t, safe)
		require.Nil(t, later)
		require.True(t, safety.valid(), "existing queries keep their proof until released")
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		release()
		require.False(t, safety.valid())
		require.NoError(t, safety.invalidate(t.Context()))
	})
}

type leasedCountSource struct {
	Source
	safety *countPushdownSafety
}

func (s leasedCountSource) countPushdownLease() (safe bool, release func()) { return s.safety.lease() }

func TestCountPushdownScannerLifetime(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var safety countPushdownSafety
		backend := &Backend{src: leasedCountSource{safety: &safety}, overTimePushdown: true}
		first := backend.ScarecrowScanner()
		_, native := first.(scarecrow.SeriesCounter)
		require.True(t, native)
		done := make(chan error, 1)
		go func() { done <- safety.invalidate(t.Context()) }()
		synctest.Wait()
		next := backend.ScarecrowScanner()
		_, native = next.(scarecrow.SeriesCounter)
		require.False(t, native)
		require.NoError(t, next.Close())
		require.True(t, safety.valid())
		require.NoError(t, first.Close())
		require.NoError(t, first.Close())
		require.NoError(t, <-done)
		require.False(t, safety.valid())
	})
}
