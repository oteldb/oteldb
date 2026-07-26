package scarecrow

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// queryableScanner adapts a row-oriented storage.Queryable to the columnar [Scanner] seam by
// draining each series' chunk iterator into a pair of slices.
//
// It costs one copy per series, which the native oteldb/storage path does not pay — that seam
// already hands over []int64/[]float64. Its purpose is to let the engine be driven by anything
// speaking the Prometheus storage interface, most importantly promqltest's corpus, without any
// storage plumbing. It is also the fallback for backends that have no columnar seam.
type queryableScanner struct {
	q storage.Queryable
}

var _ Scanner = (*queryableScanner)(nil)

// NewQueryableScanner returns a [Scanner] backed by a Prometheus storage.Queryable.
func NewQueryableScanner(q storage.Queryable) Scanner {
	return &queryableScanner{q: q}
}

func (s *queryableScanner) Close() error { return nil }

func (s *queryableScanner) Series(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]labels.Labels, error) {
	q, err := s.q.Querier(mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "create querier")
	}
	defer func() { _ = q.Close() }()

	// Sorted, so schema order is deterministic across runs and across backends.
	set := q.Select(ctx, true, hintsFor(mint, maxt), matchers...)

	var out []labels.Labels
	for set.Next() {
		out = append(out, set.At().Labels())
	}

	if err := set.Err(); err != nil {
		return nil, errors.Wrap(err, "select series")
	}

	return out, nil
}

func (s *queryableScanner) Scan(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (SeriesIterator, error) {
	q, err := s.q.Querier(mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "create querier")
	}

	return &queryableIterator{
		querier: q,
		set:     q.Select(ctx, true, hintsFor(mint, maxt), matchers...),
		mint:    mint,
		maxt:    maxt,
	}, nil
}

// hintsFor builds the select hints for a window. The engine does its own planning, so it asks
// only for the range; func-specific hints are set by the pushdown rules that need them.
func hintsFor(mint, maxt int64) *storage.SelectHints {
	return &storage.SelectHints{Start: mint, End: maxt}
}

// queryableIterator drains one series at a time from a SeriesSet, reusing its sample buffers
// across series so the adapter allocates once rather than per series.
type queryableIterator struct {
	querier storage.Querier
	set     storage.SeriesSet
	mint    int64
	maxt    int64

	cur  Samples
	iter chunkenc.Iterator
}

func (it *queryableIterator) Next(ctx context.Context) (*Samples, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for it.set.Next() {
		s := it.set.At()

		it.cur.Labels = s.Labels()
		it.cur.T = it.cur.T[:0]
		it.cur.V = it.cur.V[:0]
		it.cur.Weights = nil // a Queryable carries no sampling weights

		it.iter = s.Iterator(it.iter)
		for it.iter.Next() == chunkenc.ValFloat {
			t, v := it.iter.At()
			if t < it.mint || t > it.maxt {
				continue
			}

			it.cur.T = append(it.cur.T, t)
			it.cur.V = append(it.cur.V, v)
		}

		if err := it.iter.Err(); err != nil {
			return nil, errors.Wrap(err, "iterate samples")
		}

		// PromQL drops series with no sample in the window, so skip rather than emit empty.
		if len(it.cur.T) == 0 {
			continue
		}

		return &it.cur, nil
	}

	if err := it.set.Err(); err != nil {
		return nil, errors.Wrap(err, "advance series set")
	}

	return nil, nil
}

func (it *queryableIterator) Close() error {
	if it.querier == nil {
		return nil
	}

	err := it.querier.Close()
	it.querier = nil

	return err
}
