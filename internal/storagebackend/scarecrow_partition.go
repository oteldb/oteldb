package storagebackend

import (
	"context"
	"io"
	"sync"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func (s rawScarecrowScanner) ScanPartitions(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, workers int) ([]scarecrow.PartitionIterator, error) {
	return s.counter.ScanPartitions(ctx, mint, maxt, matchers, workers)
}

func (s *scarecrowScanner) ScanPartitions(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher, workers int) ([]scarecrow.PartitionIterator, error) {
	if workers < 1 {
		return nil, errors.New("partition scan requires a worker")
	}
	iter, err := s.openFetch(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}
	// Native fetch batches own their copied samples until Release. Only fetching is serialized;
	// conversion and folding run independently while each consumer owns one batch.
	shared := &partitionScan{iter: iter, matchers: matchers, remaining: workers}
	out := make([]scarecrow.PartitionIterator, workers)
	for i := range out {
		out[i] = &partitionIterator{shared: shared, budget: readbudget.From(ctx)}
	}
	return out, nil
}

type partitionScan struct {
	mu        sync.Mutex
	iter      fetch.Iterator
	matchers  []*labels.Matcher
	position  int
	remaining int
	done      bool
	err       error
}

func (s *partitionScan) next(ctx context.Context) (*fetch.Batch, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, 0, s.err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		batch, err := s.iter.Next(ctx)
		if err != nil {
			s.done = true
			if errors.Is(err, io.EOF) {
				return nil, 0, nil
			}
			s.err = errors.Wrap(err, "next partition batch")
			return nil, 0, s.err
		}
		if !storagepromql.MatchesAll(storagepromql.PromLabels(batch.Series), s.matchers) {
			batch.Release()
			continue
		}
		position := s.position
		s.position++
		return batch, position, nil
	}
}

type partitionIterator struct {
	shared       *partitionScan
	batch        batchIterator
	position     int
	closed       bool
	budget       *readbudget.Budget
	scratchBytes int64
	rawBytes     int64
}

func (it *partitionIterator) Position() int { return it.position }

func (it *partitionIterator) Release() {
	it.batch.release()
	it.budget.Release(it.rawBytes)
	it.rawBytes = 0
}

func (it *partitionIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	it.Release()
	batch, position, err := it.shared.next(ctx)
	if err != nil || batch == nil {
		return nil, err
	}
	it.position = position
	rawBytes := int64(cap(batch.Timestamps)+cap(batch.Values)+cap(batch.ScaleFactors)) * 8
	scratchBytes := int64(len(batch.Timestamps)) * 8
	additionalScratch := max(0, scratchBytes-it.scratchBytes)
	if err := it.budget.Reserve(rawBytes + additionalScratch); err != nil {
		batch.Release()
		return nil, err
	}
	it.rawBytes = rawBytes
	it.scratchBytes += additionalScratch
	it.batch.setBatch(batch)
	return &it.batch.cur, nil
}

func (it *partitionIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.Release()
	it.batch.ts = nil
	it.budget.Release(it.scratchBytes)
	it.scratchBytes = 0
	it.shared.mu.Lock()
	defer it.shared.mu.Unlock()
	it.shared.remaining--
	if it.shared.remaining == 0 {
		return it.shared.iter.Close()
	}
	return nil
}
