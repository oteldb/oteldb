package storagebackend

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// ScarecrowScanner returns a [scarecrow.Scanner] backed by the storage engine's native fetch seam.
//
// Unlike [Backend.MetricsScanners] (the Thanos-fork seam, itself layered over [Backend.Querier]),
// this reads [fetch.Batch] directly: no chunkenc.Iterator, no storage.Series boxing, no per-sample
// interface dispatch — the one copy this pays is the nanosecond-to-millisecond timestamp
// conversion [scarecrow.Samples] requires; Values is aliased from the batch unchanged.
func (b *Backend) ScarecrowScanner() scarecrow.Scanner {
	return &scarecrowScanner{b: b}
}

type scarecrowScanner struct {
	b *Backend
}

var _ scarecrow.Scanner = (*scarecrowScanner)(nil)

var (
	_ scarecrow.AggregateScanner = (*scarecrowScanner)(nil)
	_ scarecrow.SeriesCounter    = (*scarecrowScanner)(nil)
)

func (s *scarecrowScanner) Close() error { return nil }

// AggregateOverTime implements [scarecrow.AggregateScanner], answering a reducer `*_over_time`
// from the stats sidecar ([storage.Storage.AggregateMetricsNamed]) instead of a raw fetch-and-fold
// — the same pushdown [aggregateOverTimeOp] performs for the fork engine, reused here because both
// read the same window-aggregate API. The caller ([scarecrow]'s aggregateOverTime operator) drops
// the metric name itself, so the labels returned here are unfiltered.
func (s *scarecrowScanner) AggregateOverTime(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]scarecrow.WindowAggregate, error) {
	const nsPerMs = int64(time.Millisecond)

	// scarecrow's window is PromQL's half-open (mint, maxt]; storage's fetch range is inclusive,
	// so mint is exclusive (start = mint+1 ms) and maxt inclusive.
	aggs, err := s.b.store.AggregateMetricsNamed(ctx, s.b.tenant, fetch.Request{
		Tenant:   s.b.tenant,
		Start:    (mint + 1) * nsPerMs,
		End:      maxt * nsPerMs,
		Matchers: storagepromql.PushableMatchers(matchers),
	})
	if err != nil {
		return nil, errors.Wrap(err, "aggregate metrics")
	}

	out := make([]scarecrow.WindowAggregate, 0, len(aggs))
	for i := range aggs {
		la := &aggs[i]

		lset := storagepromql.PromLabels(la.Series)
		if !storagepromql.MatchesAll(lset, matchers) {
			continue
		}

		out = append(out, scarecrow.WindowAggregate{
			Labels: lset,
			Count:  la.Count,
			Sum:    la.Sum,
			Min:    la.Min,
			Max:    la.Max,
		})
	}

	return out, nil
}

// CountSeries implements [scarecrow.SeriesCounter], delegating to the same
// [storagepromql.Queryable]-backed CountSeries the fork engine's count() pushdown uses
// ([backendCounter]).
func (s *scarecrowScanner) CountSeries(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (uint64, error) {
	q, err := s.b.queryable().Querier(mint, maxt)
	if err != nil {
		return 0, errors.Wrap(err, "count pushdown: create querier")
	}
	defer func() { _ = q.Close() }()

	sc, ok := q.(interface {
		CountSeries(ctx context.Context, startMs, endMs int64, matchers ...*labels.Matcher) (uint64, error)
	})
	if !ok {
		return 0, nil
	}

	return sc.CountSeries(ctx, mint, maxt, matchers...)
}

// Series implements [scarecrow.Scanner].
func (s *scarecrowScanner) Series(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]labels.Labels, error) {
	batches, err := s.fetch(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}

	out := make([]labels.Labels, 0, len(batches))
	for _, b := range batches {
		out = append(out, storagepromql.PromLabels(b.Series))
		b.Release()
	}

	return out, nil
}

// Scan implements [scarecrow.Scanner].
func (s *scarecrowScanner) Scan(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (scarecrow.SeriesIterator, error) {
	batches, err := s.fetch(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}

	return &batchIterator{batches: batches}, nil
}

// fetch resolves matchers to the matching series' batches over [mint, maxt] (Prometheus
// milliseconds), pushing the index-safe matcher subset into the fetch request and re-checking the
// full matcher set against each candidate — mirroring [storagepromql.Queryable]'s Select, since a
// negated/absent matcher cannot be pushed into the postings index without wrongly excluding series
// that lack the label.
func (s *scarecrowScanner) fetch(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]*fetch.Batch, error) {
	const nsPerMs = int64(time.Millisecond)

	req := fetch.Request{
		Tenant:   s.b.tenant,
		Start:    mint * nsPerMs,
		End:      maxt * nsPerMs,
		Matchers: storagepromql.PushableMatchers(matchers),
		Recycle:  true,
	}

	it, err := s.b.store.Fetcher(s.b.tenant).Fetch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain")
	}

	kept := batches[:0]
	for _, b := range batches {
		if storagepromql.MatchesAll(storagepromql.PromLabels(b.Series), matchers) {
			kept = append(kept, b)
			continue
		}

		b.Release() // not part of the result — recycle its buffers now
	}

	return kept, nil
}

// batchIterator adapts a resolved batch slice to [scarecrow.SeriesIterator], releasing each batch
// (recycling its buffers) once the caller has moved past it — on the following Next, or on Close
// for whatever is left unreleased. [fetch.Batch.Release] is idempotent, so Close after full
// exhaustion double-releasing the last batch is safe.
type batchIterator struct {
	batches []*fetch.Batch
	i       int

	cur scarecrow.Samples
	ts  []int64 // reused ms-conversion buffer, valid only for the most recently returned Samples.
}

var _ scarecrow.SeriesIterator = (*batchIterator)(nil)

func (it *batchIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if it.i > 0 {
		it.batches[it.i-1].Release()
	}

	if it.i >= len(it.batches) {
		return nil, nil
	}

	b := it.batches[it.i]
	it.i++

	const nsPerMs = int64(time.Millisecond)

	if cap(it.ts) < len(b.Timestamps) {
		it.ts = make([]int64, len(b.Timestamps))
	}
	it.ts = it.ts[:len(b.Timestamps)]
	for j, t := range b.Timestamps {
		it.ts[j] = t / nsPerMs
	}

	it.cur = scarecrow.Samples{
		Labels:  storagepromql.PromLabels(b.Series),
		T:       it.ts,
		V:       b.Values,
		Weights: b.ScaleFactors,
	}

	return &it.cur, nil
}

func (it *batchIterator) Close() error {
	start := max(it.i-1, 0)

	for j := start; j < len(it.batches); j++ {
		it.batches[j].Release()
	}

	it.batches = nil

	return nil
}
