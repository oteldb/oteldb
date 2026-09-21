package storagebackend

import (
	"cmp"
	"context"
	"encoding/hex"
	"math"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"

	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
	sigexemplar "github.com/oteldb/storage/signal/exemplar"
)

// The label names Prometheus' OTLP translation gives an exemplar's trace context, and the ones
// Grafana follows from a sample to the trace that produced it.
const (
	exemplarTraceIDLabel = "trace_id"
	exemplarSpanIDLabel  = "span_id"
)

var _ promstorage.ExemplarQueryable = (*Backend)(nil)

// ExemplarQuerier implements [promstorage.ExemplarQueryable] over the engine's exemplars vertical.
func (b *Backend) ExemplarQuerier(ctx context.Context) (promstorage.ExemplarQuerier, error) {
	return &exemplarQuerier{b: b, ctx: ctx}, nil
}

// exemplarQuerier answers /api/v1/query_exemplars from the exemplars record engine. The Prometheus
// interface gives Select no context, so the one ExemplarQuerier was called with is carried here.
type exemplarQuerier struct {
	b   *Backend
	ctx context.Context
}

// exemplarSeriesRef locates a stream's result and records which matcher set produced it, so a
// stream several sets select is collected once rather than once per set.
type exemplarSeriesRef struct {
	out int
	set int
}

// Select implements [promstorage.ExemplarQuerier].
//
// An exemplar stream is its metric series, byte for byte, so a matcher set that selects metric
// series selects exactly the matching exemplar streams — there is no exemplar-specific selection.
// Each set is fetched separately (that is what pushes its matchers into the index), but a stream is
// kept once across all of them and the whole result is sorted by label set, as Prometheus' own
// exemplar storage does: PromQL hands Select one set per selector, so a query naming the same
// metric twice would otherwise return that series several times over.
func (q *exemplarQuerier) Select(
	startMs, endMs int64, matcherSets ...[]*labels.Matcher,
) ([]exemplar.QueryResult, error) {
	var (
		lo  = exemplarBound(startMs, math.MinInt64)
		hi  = exemplarEnd(endMs)
		ctx = q.b.queryContext(q.ctx)

		out   []exemplar.QueryResult
		index = map[signal.SeriesID]exemplarSeriesRef{}
	)

	for set, matchers := range matcherSets {
		batches, err := q.fetch(ctx, lo, hi, matchers)
		if err != nil {
			return nil, err
		}

		for _, batch := range batches {
			ref, seen := index[batch.ID]
			if seen && ref.set != set {
				continue // an earlier set already collected this stream whole
			}

			lset := storagepromql.PromLabels(batch.Series)
			// PushableMatchers drops the matchers that would wrongly prune via the postings index
			// (those matching the empty string), so the full set is re-checked here.
			if !storagepromql.MatchesAll(lset, matchers) {
				continue
			}

			exs, err := batchExemplars(batch)
			if err != nil {
				return nil, err
			}

			if len(exs) == 0 {
				continue
			}

			// One stream's rows can arrive in several batches (several parts, or several shards
			// of a cluster read), which is the one case that appends to an existing result.
			if seen {
				out[ref.out].Exemplars = append(out[ref.out].Exemplars, exs...)

				continue
			}

			index[batch.ID] = exemplarSeriesRef{out: len(out), set: set}
			out = append(out, exemplar.QueryResult{SeriesLabels: lset, Exemplars: exs})
		}
	}

	for i := range out {
		slices.SortStableFunc(out[i].Exemplars, func(a, b exemplar.Exemplar) int {
			return cmp.Compare(a.Ts, b.Ts)
		})
	}

	slices.SortStableFunc(out, func(a, b exemplar.QueryResult) int {
		return labels.Compare(a.SeriesLabels, b.SeriesLabels)
	})

	return out, nil
}

func (q *exemplarQuerier) fetch(
	ctx context.Context, lo, hi int64, matchers []*labels.Matcher,
) ([]*fetch.Batch, error) {
	it, err := q.b.src.ExemplarFetcher(q.b.tenant).Fetch(ctx, fetch.Request{
		Tenant:   q.b.tenant,
		Signal:   signal.Exemplar,
		Start:    lo,
		End:      hi,
		Matchers: storagepromql.PushableMatchers(matchers),
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch exemplars")
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain exemplars")
	}

	return batches, nil
}

// batchExemplars materializes one exemplar stream's rows.
func batchExemplars(batch *fetch.Batch) ([]exemplar.Exemplar, error) {
	values, ok := batch.Column(sigexemplar.ColValue)
	if !ok {
		return nil, nil
	}

	var (
		traceIDs = exemplarBytes(batch, sigexemplar.ColTraceID)
		spanIDs  = exemplarBytes(batch, sigexemplar.ColSpanID)
		attrs    = exemplarBytes(batch, sigexemplar.ColAttrs)

		out = make([]exemplar.Exemplar, 0, len(batch.Timestamps))
		lb  labels.ScratchBuilder
	)

	for i := range min(len(batch.Timestamps), len(values.Int64)) {
		lset, err := exemplarLabels(&lb, cellAt(attrs, i), cellAt(traceIDs, i), cellAt(spanIDs, i))
		if err != nil {
			return nil, err
		}

		out = append(out, exemplar.Exemplar{
			Labels: lset,
			Value:  sigexemplar.DecodeValue(values.Int64[i]),
			Ts:     batch.Timestamps[i] / int64(time.Millisecond),
			HasTs:  true,
		})
	}

	return out, nil
}

// exemplarLabels renders one row's exemplar labels: the aggregation-dropped attributes plus the
// trace context, which is present only when the producer had an active span.
func exemplarLabels(lb *labels.ScratchBuilder, attrs, traceID, spanID []byte) (labels.Labels, error) {
	lb.Reset()

	if len(attrs) > 0 {
		decoded, _, err := signal.DecodeAttributes(attrs)
		if err != nil {
			return labels.EmptyLabels(), errors.Wrap(err, "decode filtered attributes")
		}

		for _, kv := range decoded {
			lb.Add(string(kv.Key), valueText(kv.Value))
		}
	}

	if len(traceID) > 0 {
		lb.Add(exemplarTraceIDLabel, hex.EncodeToString(traceID))
	}

	if len(spanID) > 0 {
		lb.Add(exemplarSpanIDLabel, hex.EncodeToString(spanID))
	}

	lb.Sort()

	return lb.Labels(), nil
}

func exemplarBytes(batch *fetch.Batch, name string) [][]byte {
	c, ok := batch.Column(name)
	if !ok {
		return nil
	}

	return c.Bytes
}

func cellAt(col [][]byte, i int) []byte {
	if i >= len(col) {
		return nil
	}

	return col[i]
}

// exemplarEnd is the upper fetch bound for a query ending at endMs. The fetch window is inclusive
// nanoseconds, but what a client compares against its own end is the millisecond Ts it gets back —
// and Prometheus keeps an exemplar at `Ts == end`. So the bound covers the whole millisecond endMs
// names; a narrower one would drop an exemplar whose reported Ts is exactly endMs.
func exemplarEnd(endMs int64) int64 {
	ns := exemplarBound(endMs, math.MaxInt64)
	if ns > math.MaxInt64-int64(time.Millisecond) {
		return math.MaxInt64
	}

	return ns + int64(time.Millisecond) - 1
}

// exemplarBound converts a Prometheus millisecond bound to unix nanoseconds, collapsing a bound
// whose magnitude would overflow the conversion (the MinTime/MaxTime sentinels an unbounded query
// arrives with) to the open-ended limit.
func exemplarBound(ms, open int64) int64 {
	switch clamped := clampQueryMs(ms); clamped {
	case math.MinInt64, math.MaxInt64:
		return open
	default:
		return clamped * int64(time.Millisecond)
	}
}
