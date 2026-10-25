package scarecrow_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type countedWindowScanner struct {
	perWindowScanner
	scans atomic.Int64
}

func (s *countedWindowScanner) Scan(ctx context.Context, mint, maxt int64, m []*labels.Matcher) (scarecrow.SeriesIterator, error) {
	s.scans.Add(1)
	return s.perWindowScanner.Scan(ctx, mint, maxt, m)
}

func TestRangeCountWithoutGridScansOnce(t *testing.T) {
	t.Parallel()
	store := promqltest.LoadedStorage(t, pushdownData)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, qs := range []string{`count(http_requests)`, `count by(job)(http_requests)`, `count(count by(job)(http_requests))`} {
		t.Run(qs, func(t *testing.T) {
			var scanner *countedWindowScanner
			eng := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(q storage.Queryable) scarecrow.Scanner {
				scanner = &countedWindowScanner{perWindowScanner: perWindowScanner{s: &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}}}
				return scanner
			}})
			execRange(t, eng, store, qs, time.Unix(0, 0), time.Unix(600, 0), time.Minute)
			require.Zero(t, scanner.s.counts.Load())
			require.Zero(t, scanner.s.groupCount.Load())
			require.EqualValues(t, 1, scanner.scans.Load())
		})
	}
}

func TestPinnedCountQueriesCallOnce(t *testing.T) {
	t.Parallel()
	store := promqltest.LoadedStorage(t, pushdownData)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, qs := range []string{`count(http_requests @ 300)`, `count by(job)(http_requests @ 300)`} {
		t.Run(qs, func(t *testing.T) {
			var scanner *pushdownScanner
			eng := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, NewScanner: func(q storage.Queryable) scarecrow.Scanner {
				scanner = &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
				return scanner
			}})
			execRange(t, eng, store, qs, time.Unix(0, 0), time.Unix(600, 0), time.Minute)
			require.EqualValues(t, 1, scanner.counts.Load()+scanner.groupCount.Load())
			require.Zero(t, scanner.grids.Load())
		})
	}
}

func TestCountProjectionPreservesGapSteps(t *testing.T) {
	t.Parallel()
	const data = `load 1m
 cpu_total{cpu="0",mode="user"} _ _ 1 2 3 _ _ _ _ _ _ _ 4 5 _ _ _ _ _
 cpu_total{cpu="1",mode="idle"} _ _ 2 4 6 _ _ _ _ _ _ _ 8 10 _ _ _ _ _
 `
	store := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	start, end := time.Unix(0, 0), time.Unix(18*60, 0)
	for _, qs := range []string{
		`count(cpu_total)`, `count by(cpu)(cpu_total)`, `count(count by(cpu)(cpu_total))`,
		`sum(cpu_total) / scalar(count(count by(cpu)(cpu_total)))`,
		`scalar(count(count by(cpu)(cpu_total))) + sum(cpu_total)`,
		`sum(irate(cpu_total[75s])) / scalar(count(count by(cpu)(cpu_total)))`,
		`scalar(count(missing))`, `count by(cpu)(cpu_total @ 300)`,
	} {
		t.Run(qs, func(t *testing.T) {
			want := execRange(t, upstreamEngine(), store, qs, start, end, time.Minute)
			for _, grid := range []bool{false, true} {
				for _, chunk := range []int{-1, 1, 3, 20} {
					eng := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, ChunkSteps: chunk, NewScanner: func(q storage.Queryable) scarecrow.Scanner {
						s := &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
						if grid {
							return s
						}
						return perWindowScanner{s: s}
					}})
					got := execRange(t, eng, store, qs, start, end, time.Minute)
					require.Equalf(t, want, got, "grid=%v chunk=%d", grid, chunk)
				}
			}
		})
	}
}
