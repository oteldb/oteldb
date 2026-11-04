package scarecrow

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"
)

type partitionTestScanner struct {
	mu             sync.Mutex
	samples        []Samples
	next           int
	scans          int
	partitions     int
	released       atomic.Int64
	closed         atomic.Int64
	borrowed       atomic.Int64
	maxBorrow      atomic.Int64
	fail           error
	closeErr       error
	admit          chan struct{}
	positionOffset int
	releaseCheck   func(*Samples)
}

func (s *partitionTestScanner) Series(context.Context, int64, int64, []*labels.Matcher) ([]labels.Labels, error) {
	series := make([]labels.Labels, len(s.samples))
	for i := range series {
		series[i] = s.samples[i].Labels
	}
	return series, nil
}

func (s *partitionTestScanner) Scan(context.Context, int64, int64, []*labels.Matcher) (SeriesIterator, error) {
	s.scans++
	return &partitionTestIterator{scanner: s}, nil
}

func (s *partitionTestScanner) ScanPartitions(_ context.Context, _, _ int64, _ []*labels.Matcher, workers int) ([]PartitionIterator, error) {
	s.partitions++
	parts := make([]PartitionIterator, workers)
	for i := range parts {
		parts[i] = &partitionTestIterator{scanner: s}
	}
	return parts, nil
}

func (*partitionTestScanner) Close() error { return nil }

type partitionTestIterator struct {
	scanner *partitionTestScanner
	pos     int
	current *Samples
	closed  bool
}

func (it *partitionTestIterator) Position() int { return it.pos }

func (it *partitionTestIterator) Next(ctx context.Context) (*Samples, error) {
	it.Release()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := it.scanner
	if s.admit != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case s.admit <- struct{}{}:
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next == len(s.samples) {
		if s.admit != nil {
			<-s.admit
		}
		return nil, s.fail
	}
	it.pos = s.next + s.positionOffset
	it.current = &s.samples[s.next]
	s.next++
	n := s.borrowed.Add(1)
	for {
		old := s.maxBorrow.Load()
		if n <= old || s.maxBorrow.CompareAndSwap(old, n) {
			break
		}
	}
	return it.current, nil
}

func (it *partitionTestIterator) Release() {
	if it.current == nil {
		return
	}
	samples := it.current
	it.current = nil
	if it.scanner.releaseCheck != nil {
		it.scanner.releaseCheck(samples)
	}
	it.scanner.borrowed.Add(-1)
	it.scanner.released.Add(1)
	if it.scanner.admit != nil {
		<-it.scanner.admit
	}
}

func (it *partitionTestIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.Release()
	it.scanner.closed.Add(1)
	return it.scanner.closeErr
}

func parallelTestFold(s Scanner) *matrixFold {
	steps := make([]int64, 64)
	for i := range steps {
		steps[i] = int64(i+1) * 1000
	}
	ec := &EvalContext{Steps: steps, Interval: time.Second}
	fold := newMatrixFold(&scannerSource{scanner: s, mint: 0, maxt: 64_000}, "test", "sum_over_time",
		rangeFuncs["sum_over_time"], time.Second, 0, nil, nil, ec)
	fold.slots = newSemaphore(4)
	return fold
}

func parallelTestSamples() []Samples {
	out := make([]Samples, 40)
	for i := range out {
		out[i] = Samples{Labels: labels.FromStrings("__name__", "test", "instance", string(rune('a'+i)))}
		for j := range 64 {
			out[i].T = append(out[i].T, int64(j+1)*1000)
			out[i].V = append(out[i].V, float64(i+1))
		}
	}
	// Empty output must still participate in ordering and release its raw borrow.
	out[7].T, out[7].V = []int64{-1000}, []float64{42}
	return out
}

func TestParallelFoldOrderAndOwnership(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	s := &partitionTestScanner{samples: parallelTestSamples()}
	fold := parallelTestFold(s)
	defer func() { require.NoError(t, fold.Close()) }()
	for i := range 40 {
		if i == 7 {
			continue
		}
		col, err := fold.Next(t.Context())
		require.NoError(t, err)
		require.Equal(t, SeriesRef(i), col.Ref)
		require.Equal(t, float64(i+1), col.V[0])
	}
	col, err := fold.Next(t.Context())
	require.NoError(t, err)
	require.Nil(t, col)
	require.NoError(t, fold.Close())
	require.Equal(t, 1, s.partitions)
	require.Zero(t, s.scans)
	require.EqualValues(t, 40, s.released.Load())
	require.Zero(t, s.borrowed.Load())
	require.LessOrEqual(t, s.maxBorrow.Load(), int64(4))
	require.EqualValues(t, 4, s.closed.Load())
}

func TestParallelFoldOverlapsSeries(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	s := &partitionTestScanner{samples: parallelTestSamples()}
	fold := parallelTestFold(s)
	barrier := make(chan struct{})
	var entered atomic.Int64
	fn := fold.fn
	fold.fn = func(w *window, param float64) (float64, bool) {
		switch entered.Add(1) {
		case 1:
			<-barrier
		case 2:
			close(barrier)
		}
		return fn(w, param)
	}
	_, err := fold.Next(t.Context())
	require.NoError(t, err)
	require.NoError(t, fold.Close())
	require.GreaterOrEqual(t, s.maxBorrow.Load(), int64(2))
}

func TestParallelFoldReleasesBeforeWaitingForColumns(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	// This source admits only one outstanding raw batch. Publishing a column while retaining
	// that borrow would prevent another worker from obtaining the next batch forever.
	s := &partitionTestScanner{samples: parallelTestSamples(), admit: make(chan struct{}, 1)}
	fold := parallelTestFold(s)
	budget := readbudget.New(4 * (64*8 + 8))
	ctx := readbudget.With(t.Context(), budget)
	for {
		col, err := fold.Next(ctx)
		require.NoError(t, err)
		if col == nil {
			break
		}
	}
	require.NotNil(t, fold.parallel)
	require.NoError(t, fold.Close())
	require.EqualValues(t, 1, s.maxBorrow.Load())
	require.EqualValues(t, 4*(64*8+8), budget.Remaining())
}

func TestParallelFoldClearsBorrowedWindowBeforeRelease(t *testing.T) {
	s := &partitionTestScanner{samples: parallelTestSamples()[:1]}
	s.samples[0].V[0] = math.Float64frombits(0x7ff0000000000002)
	s.samples[0].Weights = make([]float64, len(s.samples[0].V))
	for i := range s.samples[0].Weights {
		s.samples[0].Weights[i] = 1
	}
	op := parallelTestFold(s)
	_, err := op.Schema(t.Context())
	require.NoError(t, err)
	budget := readbudget.New(1 << 20)
	worker := &foldWorker{iter: &partitionTestIterator{scanner: s}, op: *op, ack: make(chan struct{}, 1), budget: budget}
	var retained atomic.Bool
	s.releaseCheck = func(samples *Samples) {
		w := &worker.op
		if w.w.T != nil || w.w.V != nil || w.w.W != nil {
			retained.Store(true)
		}
		clear(samples.V)
		clear(samples.Weights)
	}
	ctx, cancel := context.WithCancel(t.Context())
	results := make(chan foldResult)
	var wg sync.WaitGroup
	wg.Go(func() { worker.run(ctx, results) })
	result := <-results
	require.NotNil(t, result.col)
	require.False(t, result.col.IsSet(0))
	require.Equal(t, float64(1), result.col.V[1], "poisoning released samples must not change the folded column")
	require.EqualValues(t, (1<<20)-64*24, budget.Remaining(), "owned stale-window scratch remains charged while idle")
	cancel()
	wg.Wait()
	require.False(t, retained.Load(), "an idle worker must not retain raw arrays")
	require.Nil(t, worker.op.wt)
	require.Nil(t, worker.op.wv)
	require.Nil(t, worker.op.ww)
	require.EqualValues(t, 1<<20, budget.Remaining())
}

func TestParallelFoldRejectsOrdinalGap(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	s := &partitionTestScanner{samples: parallelTestSamples(), positionOffset: 1}
	fold := parallelTestFold(s)
	_, err := fold.Next(t.Context())
	require.ErrorContains(t, err, "gap in series positions")
	_, repeated := fold.Next(t.Context())
	require.ErrorIs(t, repeated, err)
	require.NoError(t, fold.Close())
}

func TestParallelFoldFallback(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, name := range []string{"queryable", "slots", "budget", "one-core", "mad"} {
		t.Run(name, func(t *testing.T) {
			s := &partitionTestScanner{samples: parallelTestSamples()}
			var scanner Scanner = s
			ctx := t.Context()
			var slots *semaphore
			switch name {
			case "queryable":
				scanner = struct{ Scanner }{s}
			case "slots":
				slots = newSemaphore(4)
				for slots.tryAcquire() {
					t.Cleanup(slots.release)
				}
			case "budget":
				ctx = readbudget.With(ctx, readbudget.New(1))
			case "one-core":
				old := runtime.GOMAXPROCS(1)
				t.Cleanup(func() { runtime.GOMAXPROCS(old) })
			}
			fold := parallelTestFold(scanner)
			if name == "mad" {
				fold.fnName, fold.fn = "mad_over_time", rangeFuncs["mad_over_time"]
			}
			if slots != nil {
				fold.slots = slots
			}
			for {
				col, err := fold.Next(ctx)
				require.NoError(t, err)
				if col == nil {
					break
				}
			}
			require.NoError(t, fold.Close())
			require.Equal(t, 1, s.scans)
			require.Zero(t, s.partitions)
		})
	}
}

func TestParallelFoldAggregateFloatOrder(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, op := range []parser.ItemType{parser.SUM, parser.AVG, parser.STDVAR, parser.MIN, parser.MAX, parser.COUNT} {
		t.Run(op.String(), func(t *testing.T) {
			var results []uint64
			for _, parallel := range []bool{false, true} {
				s := &partitionTestScanner{samples: parallelTestSamples()}
				values := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1e-300, math.Copysign(0, -1)}
				for i := range s.samples {
					for j := range s.samples[i].V {
						s.samples[i].V[j] = values[i%len(values)]
					}
				}
				var scanner Scanner = s
				if !parallel {
					scanner = struct{ Scanner }{s}
				}
				fold := parallelTestFold(scanner)
				aggregate := newAggregate(fold, &parser.AggregateExpr{Op: op}, fold.ec)
				col, err := aggregate.Next(t.Context())
				require.NoError(t, err)
				for _, v := range col.V {
					results = append(results, math.Float64bits(v))
				}
				require.NoError(t, aggregate.Close())
			}
			require.Equal(t, results[:64], results[64:])
		})
	}
}

func TestParallelFoldDenseWeightedStaleWindows(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, name := range []string{"sum_over_time", "avg_over_time", "count_over_time", "stddev_over_time", "stdvar_over_time", "min_over_time", "max_over_time", "rate", "irate", "delta", "changes", "resets"} {
		t.Run(name, func(t *testing.T) {
			var want [][]uint64
			for _, parallel := range []bool{false, true} {
				s := &partitionTestScanner{samples: parallelTestSamples()}
				for i := range s.samples {
					samples := &s.samples[i]
					samples.Weights = make([]float64, len(samples.V))
					for j := range samples.V {
						samples.Weights[j] = float64(j%3 + 1)
						samples.V[j] = float64((i*17 + j*3) % 23)
						if j%11 == 0 {
							samples.V[j] = math.Float64frombits(0x7ff0000000000002)
						}
					}
				}
				var scanner Scanner = s
				if !parallel {
					scanner = struct{ Scanner }{s}
				}
				fold := parallelTestFold(scanner)
				fold.rng = time.Minute
				fold.fnName, fold.fn = name, rangeFuncs[name]
				budget := readbudget.New(1 << 20)
				ctx := readbudget.With(t.Context(), budget)
				var got [][]uint64
				for {
					col, err := fold.Next(ctx)
					require.NoError(t, err)
					if col == nil {
						break
					}
					bits := append([]uint64{uint64(col.Ref)}, col.Valid...)
					for _, v := range col.V {
						bits = append(bits, math.Float64bits(v))
					}
					got = append(got, bits)
				}
				require.NoError(t, fold.Close())
				require.EqualValues(t, 1<<20, budget.Remaining())
				if parallel {
					require.Equal(t, want, got)
				} else {
					want = got
				}
			}
		})
	}
}

func TestParallelFoldCloseAndErrors(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, name := range []string{"early", "cancel", "scan", "close", "samples"} {
		t.Run(name, func(t *testing.T) {
			sentinel := errors.New("test failure")
			s := &partitionTestScanner{samples: parallelTestSamples()}
			fold := parallelTestFold(s)
			budget := readbudget.New(1 << 20)
			ctx, cancel := context.WithCancel(readbudget.With(t.Context(), budget))
			defer cancel()
			switch name {
			case "scan":
				s.fail = sentinel
			case "close":
				s.closeErr = sentinel
			case "samples":
				fold.ec.Budget = newSampleBudget(1)
			}
			_, err := fold.Next(ctx)
			if name == "samples" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if name == "cancel" {
				cancel()
				_, err = fold.Next(ctx)
				require.ErrorIs(t, err, context.Canceled)
			}
			if name == "scan" {
				for err == nil {
					_, err = fold.Next(ctx)
				}
				require.ErrorIs(t, err, sentinel)
			}
			err = fold.Close()
			if name == "close" {
				require.ErrorIs(t, err, sentinel)
			}
			require.Zero(t, s.borrowed.Load())
			require.EqualValues(t, 4, s.closed.Load())
			require.EqualValues(t, 1<<20, budget.Remaining())
			require.NoError(t, fold.parallel.budget.Reserve(1<<20))
			fold.parallel.budget.Release(1 << 20)
			require.Equal(t, err, fold.Close())
		})
	}
}
