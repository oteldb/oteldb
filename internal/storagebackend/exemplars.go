package storagebackend

import (
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

// Select implements [promstorage.ExemplarQuerier].
//
// An exemplar stream is its metric series, byte for byte, so a matcher set that selects metric
// series selects exactly the matching exemplar streams — there is no exemplar-specific selection.
// Matcher sets are answered independently, as Prometheus does, but the streams a single set matches
// are merged by series so one series yields one result even when its rows span several parts.
func (q *exemplarQuerier) Select(
	startMs, endMs int64, matcherSets ...[]*labels.Matcher,
) ([]exemplar.QueryResult, error) {
	var (
		lo  = exemplarBound(startMs, math.MinInt64)
		hi  = exemplarBound(endMs, math.MaxInt64)
		ctx = q.b.src.WithQueryBudget(q.ctx)
		out []exemplar.QueryResult
	)

	for _, matchers := range matcherSets {
		res, err := q.selectOne(ctx, lo, hi, matchers)
		if err != nil {
			return nil, err
		}

		out = append(out, res...)
	}

	return out, nil
}

func (q *exemplarQuerier) selectOne(
	ctx context.Context, lo, hi int64, matchers []*labels.Matcher,
) ([]exemplar.QueryResult, error) {
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

	var (
		out   = make([]exemplar.QueryResult, 0, len(batches))
		index = make(map[signal.SeriesID]int, len(batches))
	)

	for _, batch := range batches {
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

		if i, ok := index[batch.ID]; ok {
			out[i].Exemplars = append(out[i].Exemplars, exs...)

			continue
		}

		index[batch.ID] = len(out)
		out = append(out, exemplar.QueryResult{SeriesLabels: lset, Exemplars: exs})
	}

	for i := range out {
		slices.SortStableFunc(out[i].Exemplars, func(a, b exemplar.Exemplar) int {
			return int(a.Ts - b.Ts)
		})
	}

	slices.SortStableFunc(out, func(a, b exemplar.QueryResult) int {
		return labels.Compare(a.SeriesLabels, b.SeriesLabels)
	})

	return out, nil
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
