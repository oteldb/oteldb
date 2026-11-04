package scarecrow

import (
	"context"
	"runtime"
	"slices"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/readbudget"
)

// parallelFold merges completed folds in storage order. SUM and AVG retain the serial Kahan
// and overflow behavior: merging private floating-point aggregates would change their results.
type parallelFold struct {
	workers []*foldWorker
	last    *foldWorker
	results chan foldResult
	pending map[int]foldResult
	next    int
	done    int
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	budget  *readbudget.Budget
	bytes   int64
	once    sync.Once
	err     error
	nextErr error
}

type foldWorker struct {
	iter         PartitionIterator
	op           matrixFold
	ack          chan struct{}
	err          error
	closeErr     error
	budget       *readbudget.Budget
	scratchBytes int64
}

type foldResult struct {
	worker   *foldWorker
	col      *Column
	position int
	err      error
}

// parallelRangeFuncs contains folds that allocate no private per-window scratch. Functions
// added to rangeFuncs stay serial until their memory behavior has been checked.
var parallelRangeFuncs = map[string]bool{
	"count_over_time":       true,
	"sum_over_time":         true,
	"avg_over_time":         true,
	"min_over_time":         true,
	"max_over_time":         true,
	"last_over_time":        true,
	"first_over_time":       true,
	"present_over_time":     true,
	"stddev_over_time":      true,
	"stdvar_over_time":      true,
	"rate":                  true,
	"increase":              true,
	"delta":                 true,
	"irate":                 true,
	"idelta":                true,
	"changes":               true,
	"resets":                true,
	"deriv":                 true,
	"predict_linear":        true,
	"ts_of_first_over_time": true,
	"ts_of_last_over_time":  true,
	"ts_of_max_over_time":   true,
	"ts_of_min_over_time":   true,
}

func (o *matrixFold) startParallel(ctx context.Context) error {
	o.parallelTried = true
	src, ok := o.src.(*scannerSource)
	if !ok || !parallelRangeFuncs[o.fnName] || o.schema.Len() < 4 || o.ec.NumSteps() < 32 {
		return nil
	}
	scanner, ok := src.scanner.(PartitionScanner)
	if !ok {
		return nil
	}
	degree := min(runtime.GOMAXPROCS(0), o.schema.Len())
	acquired := 0
	for acquired < degree && o.slots.tryAcquire() {
		acquired++
	}
	if acquired < 2 {
		for range acquired {
			o.slots.release()
		}
		return nil
	}
	budget := readbudget.From(ctx)
	bytes := int64(acquired) * int64(o.ec.NumSteps()*8+wordsFor(o.ec.NumSteps())*8)
	if err := budget.Reserve(bytes); err != nil {
		for range acquired {
			o.slots.release()
		}
		// Memory pressure, like slot pressure, trades parallelism for serial progress.
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	iters, err := scanner.ScanPartitions(workerCtx, src.mint, src.maxt, src.matchers, acquired)
	if err != nil || len(iters) != acquired {
		cancel()
		for _, it := range iters {
			_ = it.Close()
		}
		budget.Release(bytes)
		for range acquired {
			o.slots.release()
		}
		if err != nil {
			return err
		}
		return errors.New("partition scanner returned an unexpected worker count")
	}
	p := &parallelFold{
		cancel: cancel, budget: budget, bytes: bytes,
		results: make(chan foldResult), pending: make(map[int]foldResult, acquired),
	}
	for _, it := range iters {
		worker := &foldWorker{
			iter:   it,
			budget: budget,
			op: matrixFold{
				fn: o.fn, fnName: o.fnName, rng: o.rng, refs: o.refs, ec: o.ec,
				schema: o.schema, byHash: o.byHash, paramVals: o.paramVals,
			},
			ack: make(chan struct{}, 1),
		}
		p.workers = append(p.workers, worker)
		p.wg.Go(func() {
			defer o.slots.release()
			worker.run(workerCtx, p.results)
		})
	}
	o.parallel = p
	return nil
}

func (w *foldWorker) run(ctx context.Context, results chan<- foldResult) {
	defer func() {
		w.op.w = window{}
		w.op.wt, w.op.wv, w.op.ww = nil, nil, nil
		w.budget.Release(w.scratchBytes)
		w.scratchBytes = 0
		w.closeErr = w.iter.Close()
		select {
		case <-ctx.Done():
		case results <- foldResult{worker: w, err: w.err}:
		}
	}()
	for {
		samples, err := w.iter.Next(ctx)
		if err != nil || samples == nil {
			w.err = err
			return
		}
		if err := w.op.ec.charge(len(samples.T)); err != nil {
			w.err = err
			w.iter.Release()
			return
		}
		ref, ok := w.op.refFor(samples.Labels)
		if !ok {
			w.err = errors.Errorf("series %s absent from resolved schema", samples.Labels)
			w.iter.Release()
			return
		}
		w.op.out.Resize(ref, w.op.ec.NumSteps())
		if slices.ContainsFunc(samples.V, isStale) {
			n := len(samples.T)
			tcap, vcap, wcap := max(n, cap(w.op.wt)), max(n, cap(w.op.wv)), cap(w.op.ww)
			if samples.Weights != nil {
				wcap = max(n, wcap)
			}
			bytes := int64(tcap+vcap+wcap) * 8
			if err := w.budget.Reserve(bytes - w.scratchBytes); err != nil {
				w.err = err
				w.iter.Release()
				return
			}
			w.scratchBytes = bytes
			if cap(w.op.wt) < n {
				w.op.wt = make([]int64, 0, n)
			}
			if cap(w.op.wv) < n {
				w.op.wv = make([]float64, 0, n)
			}
			if cap(w.op.ww) < wcap {
				w.op.ww = make([]float64, 0, wcap)
			}
		}
		w.op.fold(samples)
		position := w.iter.Position()
		w.op.w = window{}
		// Never wait on a column consumer while holding raw storage buffers.
		w.iter.Release()
		select {
		case <-ctx.Done():
			w.err = ctx.Err()
			return
		case results <- foldResult{worker: w, col: &w.op.out, position: position}:
		}
		select {
		case <-ctx.Done():
			w.err = ctx.Err()
			return
		case <-w.ack:
		}
	}
}

func (p *parallelFold) Next(ctx context.Context) (*Column, error) {
	if p.nextErr != nil {
		return nil, p.nextErr
	}
	col, err := p.nextColumn(ctx)
	if err != nil {
		p.nextErr = err
		p.cancel()
	}
	return col, err
}

func (p *parallelFold) nextColumn(ctx context.Context) (*Column, error) {
	for {
		if p.last != nil {
			p.last.ack <- struct{}{}
			p.last = nil
		}
		result, ok := p.pending[p.next]
		for !ok {
			if p.done == len(p.workers) {
				if len(p.pending) != 0 {
					return nil, errors.New("partition scan has a gap in series positions")
				}
				return nil, nil
			}
			if len(p.pending)+p.done == len(p.workers) {
				return nil, errors.New("partition scan has a gap in series positions")
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case received := <-p.results:
				if received.col == nil {
					p.done++
					if received.err != nil {
						return nil, received.err
					}
				} else {
					if _, exists := p.pending[received.position]; exists || received.position < p.next {
						return nil, errors.New("partition scan returned a duplicate series position")
					}
					p.pending[received.position] = received
				}
			}
			result, ok = p.pending[p.next]
		}
		delete(p.pending, p.next)
		p.next++
		p.last = result.worker
		if !result.col.Empty() {
			return result.col, nil
		}
	}
}

func (p *parallelFold) Close() error {
	p.once.Do(func() {
		p.cancel()
		p.wg.Wait()
		for _, w := range p.workers {
			p.err = errors.Join(p.err, w.closeErr)
		}
		p.budget.Release(p.bytes)
	})
	return p.err
}
