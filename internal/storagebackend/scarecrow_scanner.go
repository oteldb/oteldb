package storagebackend

import (
	"context"
	"io"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/engine"
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
	return &scarecrowScanner{b: b, scope: fetch.NewScope()}
}

type scarecrowScanner struct {
	b *Backend
	// scope ties every read this scanner makes to one query. The engine builds a scanner per
	// query execution and closes it with the query, so the scanner *is* the session: storage's
	// decode-budget admission needs that identity to tell "this query again" from "another
	// query", and without it a query holding several fetches open deadlocks against its own
	// reservation (oteldb/storage#284).
	scope *fetch.Scope
}

var _ scarecrow.Scanner = (*scarecrowScanner)(nil)

var (
	_ scarecrow.AggregateScanner     = (*scarecrowScanner)(nil)
	_ scarecrow.SeriesCounter        = (*scarecrowScanner)(nil)
	_ scarecrow.GroupedSeriesCounter = (*scarecrowScanner)(nil)
	_ scarecrow.GridAggregateScanner = (*scarecrowScanner)(nil)
)

func (s *scarecrowScanner) Close() error { return nil }

// AggregateGrid implements [scarecrow.GridAggregateScanner]: it answers a whole PromQL step grid
// with one [storage.Storage.AggregateMetricsWindowNamed] call, which folds each series' samples
// once into step-wide buckets and slides them into every overlapping window.
//
// This is the same windowed call the fork engine's range `*_over_time` pushdown uses
// ([aggregateOverTimeRangeOp]); the per-window entry points above predate it and cost one storage
// round-trip per step, which measured ~240x slower than no pushdown at all on a 241-step grid.
//
// The grid's anchor matters: storage would otherwise evaluate on the absolute grid (multiples of
// Step from the epoch) and answer at timestamps the query never asked about.
func (s *scarecrowScanner) AggregateGrid(
	ctx context.Context, grid scarecrow.WindowGrid, matchers []*labels.Matcher,
) ([]scarecrow.GridAggregate, error) {
	const nsPerMs = int64(time.Millisecond)

	// The request must span every window the grid touches: from the first window's exclusive
	// start to the last window's inclusive end.
	var (
		firstEnd = grid.Start
		lastEnd  = grid.Start + int64(grid.NumSteps-1)*grid.Step
	)

	named, err := s.b.src.AggregateMetricsWindowNamed(ctx, s.b.tenant, fetch.Request{
		Tenant: s.b.tenant,
		Scope:  s.scope,
		// Lead-in: the first window opens a full width before the first step.
		Start:    (firstEnd - grid.Width + 1) * nsPerMs,
		End:      lastEnd * nsPerMs,
		Matchers: storagepromql.PushableMatchers(matchers),
	}, engine.WindowSpec{
		Step:   grid.Step * nsPerMs,
		Window: grid.Width * nsPerMs,
		Anchor: grid.Start * nsPerMs,
	})
	if err != nil {
		return nil, errors.Wrap(err, "aggregate metrics window")
	}

	out := make([]scarecrow.GridAggregate, 0, len(named))

	for i := range named {
		lset := storagepromql.PromLabels(named[i].Series)
		if !storagepromql.MatchesAll(lset, matchers) {
			continue
		}

		// Windows are keyed by their evaluation timestamp, not by position: the request reaches a
		// full width back before the first step, so storage legitimately returns windows ending
		// before it, and a step the series has no sample in is absent entirely. Indexing
		// positionally silently shifts every value onto the wrong step whenever the two disagree
		// — which is what TestScarecrowScannerGridMatchesFork caught at a step that does not
		// divide the window evenly.
		windows := make([]scarecrow.Aggregate, grid.NumSteps)

		for _, w := range named[i].Windows {
			endMs := w.End / nsPerMs

			step := (endMs - grid.Start) / grid.Step
			if step < 0 || step >= int64(grid.NumSteps) {
				continue
			}

			if grid.Start+step*grid.Step != endMs {
				continue // Not on this query's grid.
			}

			windows[step] = scarecrow.Aggregate{
				Count: w.Count,
				Sum:   w.Sum,
				Min:   w.Min,
				Max:   w.Max,
			}
		}

		out = append(out, scarecrow.GridAggregate{Labels: lset, Windows: windows})
	}

	return out, nil
}

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
	aggs, err := s.b.src.AggregateMetricsNamed(ctx, s.b.tenant, fetch.Request{
		Tenant:   s.b.tenant,
		Scope:    s.scope,
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
			Aggregate: scarecrow.Aggregate{
				Count: la.Count,
				Sum:   la.Sum,
				Min:   la.Min,
				Max:   la.Max,
			},
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
	q, err := s.b.queryable().QuerierWithScope(mint, maxt, s.scope)
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

// CountSeriesBy implements [scarecrow.GroupedSeriesCounter], delegating to the same
// [storagepromql.Queryable]-backed CountSeriesBy the fork engine's count-by pushdown uses
// ([backendGroupCounter]).
func (s *scarecrowScanner) CountSeriesBy(
	ctx context.Context, mint, maxt int64, label string, matchers []*labels.Matcher,
) (map[string]uint64, error) {
	q, err := s.b.queryable().QuerierWithScope(mint, maxt, s.scope)
	if err != nil {
		return nil, errors.Wrap(err, "count-by pushdown: create querier")
	}
	defer func() { _ = q.Close() }()

	sc, ok := q.(interface {
		CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
	})
	if !ok {
		return nil, errors.New("count-by pushdown: querier does not implement CountSeriesBy")
	}

	return sc.CountSeriesBy(ctx, mint, maxt, label, matchers...)
}

// Series implements [scarecrow.Scanner], answering from the series index rather than by reading
// samples: [storage.Storage.MetricSeries] is the metrics twin of LogSeries, so enumerating the
// selector's schema costs O(matching series) instead of (cardinality x window).
//
// Fetching instead — which this did until oteldb/storage#262 gave metrics an index seam — decoded
// every sample of every matching series and threw them all away to keep the labels. That is what
// made `{__name__=~".+"}` over 3h fail: the schema pass alone exhausted the process before the
// engine's sample budget had been charged a single sample.
func (s *scarecrowScanner) Series(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) ([]labels.Labels, error) {
	const nsPerMs = int64(time.Millisecond)

	series, err := s.b.src.MetricSeries(
		ctx, s.b.tenant, storagepromql.PushableMatchers(matchers), mint*nsPerMs, maxt*nsPerMs,
	)
	if err != nil {
		return nil, errors.Wrap(err, "metric series")
	}

	out := make([]labels.Labels, 0, len(series))

	for i := range series {
		// The pushed subset is a superset filter: a negated or absent matcher cannot go into the
		// postings index without wrongly excluding series that lack the label, so re-check here.
		lset := storagepromql.PromLabels(series[i])
		if !storagepromql.MatchesAll(lset, matchers) {
			continue
		}

		out = append(out, lset)
	}

	return out, nil
}

// Scan implements [scarecrow.Scanner]. The [fetch.Iterator] is handed to the returned
// [scarecrow.SeriesIterator] unconsumed, so series arrive one at a time.
func (s *scarecrowScanner) Scan(
	ctx context.Context, mint, maxt int64, matchers []*labels.Matcher,
) (scarecrow.SeriesIterator, error) {
	const nsPerMs = int64(time.Millisecond)

	it, err := s.b.src.Fetcher(s.b.tenant).Fetch(ctx, fetch.Request{
		Tenant:   s.b.tenant,
		Scope:    s.scope,
		Start:    mint * nsPerMs,
		End:      maxt * nsPerMs,
		Matchers: storagepromql.PushableMatchers(matchers),
		Recycle:  true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	return &batchIterator{it: it, matchers: matchers}, nil
}

// batchIterator adapts a [fetch.Iterator] to [scarecrow.SeriesIterator], one series at a time.
//
// It deliberately does not drain: the engine folds each series onto the step grid and moves on
// (see [scarecrow.Samples]), so streaming keeps the raw level O(1) in series, and — the reason it
// matters — lets the query's sample budget reject an oversized scan partway instead of after the
// whole result set is already resident. Draining first made the budget unreachable.
//
// A batch is released once the caller has moved past it — on the following Next, or on Close for
// whichever one is still outstanding — because the [scarecrow.Samples] it yields aliases the
// batch's Values. [fetch.Batch.Release] is idempotent, so Close after exhaustion is safe.
type batchIterator struct {
	it       fetch.Iterator
	matchers []*labels.Matcher

	prev *fetch.Batch
	cur  scarecrow.Samples
	ts   []int64 // reused ms-conversion buffer, valid only for the most recently returned Samples.
}

var _ scarecrow.SeriesIterator = (*batchIterator)(nil)

func (it *batchIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	for {
		b, err := it.it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				it.release()

				return nil, nil
			}

			return nil, errors.Wrap(err, "next batch")
		}

		lset := storagepromql.PromLabels(b.Series)

		// See [scarecrowScanner.Series]: the pushed matcher subset admits a superset.
		if !storagepromql.MatchesAll(lset, it.matchers) {
			b.Release()

			continue
		}

		it.release()
		it.prev = b

		const nsPerMs = int64(time.Millisecond)

		if cap(it.ts) < len(b.Timestamps) {
			it.ts = make([]int64, len(b.Timestamps))
		}
		it.ts = it.ts[:len(b.Timestamps)]
		for j, t := range b.Timestamps {
			it.ts[j] = t / nsPerMs
		}

		it.cur = scarecrow.Samples{
			Labels:  lset,
			T:       it.ts,
			V:       b.Values,
			Weights: b.ScaleFactors,
		}

		return &it.cur, nil
	}
}

// release recycles the batch the previous Next handed out, whose buffers the caller is done with.
func (it *batchIterator) release() {
	if it.prev == nil {
		return
	}

	it.prev.Release()
	it.prev = nil
}

func (it *batchIterator) Close() error {
	it.release()

	return it.it.Close()
}
