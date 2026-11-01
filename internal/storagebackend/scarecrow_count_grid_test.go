package storagebackend_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type countGridSpy struct {
	scarecrow.GridSeriesCounter

	counter scarecrow.SeriesCounter
	grouped scarecrow.GroupedSeriesCounter
	calls   *atomic.Int64
}

func (s countGridSpy) CountSeries(ctx context.Context, start, end int64, matchers []*labels.Matcher) (uint64, error) {
	return s.counter.CountSeries(ctx, start, end, matchers)
}

func (s countGridSpy) CountSeriesBy(ctx context.Context, start, end int64, label string, matchers []*labels.Matcher) (map[string]uint64, error) {
	return s.grouped.CountSeriesBy(ctx, start, end, label, matchers)
}

func (s countGridSpy) CountSeriesGrid(ctx context.Context, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) (map[string][]uint64, error) {
	s.calls.Add(1)
	return s.GridSeriesCounter.CountSeriesGrid(ctx, grid, label, matchers)
}

func TestScarecrowNativeCountGridRouting(t *testing.T) {
	t.Parallel()
	base := time.Unix(2_000_000, 0).UTC()
	backend := ingestScannerFixture(t.Context(), t, base)
	var calls atomic.Int64
	native := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner {
		s := backend.ScarecrowScanner()
		return countGridSpy{
			GridSeriesCounter: s.(scarecrow.GridSeriesCounter),
			counter:           s.(scarecrow.SeriesCounter), grouped: s.(scarecrow.GroupedSeriesCounter), calls: &calls,
		}
	}})
	raw := scarecrow.NewEngine(scarecrow.Opts{})
	for _, query := range []string{
		`count(scan_metric)`, `count by(foo)(scan_metric)`, `count(count by(foo)(scan_metric))`,
		`count(scan_metric{__kind__="0"})`, `count by(__kind__)(scan_metric)`,
	} {
		t.Run(query, func(t *testing.T) {
			calls.Store(0)
			start, end := base.Add(-30*time.Second), base.Add(30*time.Second)
			reference, err := raw.NewRangeQuery(t.Context(), backend, nil, query, start, end, 15*time.Second)
			require.NoError(t, err)
			defer reference.Close()
			actual, err := native.NewRangeQuery(t.Context(), backend, nil, query, start, end, 15*time.Second)
			require.NoError(t, err)
			defer actual.Close()
			want, got := reference.Exec(t.Context()), actual.Exec(t.Context())
			require.NoError(t, want.Err)
			require.NoError(t, got.Err)
			require.Equal(t, want.Value.String(), got.Value.String())
			require.Equal(t, int64(1), calls.Load(), "one count-grid attempt for every step; unsupported projections fall back")
		})
	}
}
