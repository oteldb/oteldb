package storagebackend

import (
	"context"
	"io"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type traceExtent struct {
	start, end time.Time
}

func needsTraceExtents(params traceqlengine.SelectSpansetsParams) bool {
	if !params.Start.IsZero() || !params.End.IsZero() || params.MinDuration != 0 || params.MaxDuration != 0 {
		return true
	}
	for _, matcher := range params.TraceDurationMatchers {
		if matcher.Attribute.Prop == traceql.TraceDuration && matcher.Op != 0 {
			return true
		}
	}
	return false
}

func (q *TraceQuerier) filterTraceExtents(
	ctx context.Context, params traceqlengine.SelectSpansetsParams, ids map[otelstorage.TraceID]struct{},
) (map[otelstorage.TraceID]struct{}, error) {
	if len(ids) == 0 {
		return ids, nil
	}
	lo, hi := fetchWindow(time.Time{}, time.Time{})
	it, err := q.b.src.TraceFetcher(q.b.tenant).Fetch(ctx, fetch.Request{
		Tenant: q.b.tenant, Signal: signal.Trace, Start: lo, End: hi,
		Projection: []string{sigtrace.ColTraceID, sigtrace.ColDuration},
		Conditions: []fetch.Condition{traceIDCondition(ids)}, AllConditions: true, Recycle: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch trace extents")
	}
	defer func() { _ = it.Close() }()

	extents := make(map[otelstorage.TraceID]traceExtent, len(ids))
	for {
		batch, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrap(err, "read trace extents")
		}
		traceIDs, ok := batch.Column(sigtrace.ColTraceID)
		if !ok {
			batch.Release()
			continue
		}
		durations, _ := batch.Column(sigtrace.ColDuration)
		for i, ts := range batch.Timestamps {
			if i >= len(traceIDs.Bytes) {
				continue
			}
			id := otelTraceID(traceIDs.Bytes[i])
			if _, ok := ids[id]; !ok {
				continue
			}
			var duration int64
			if i < len(durations.Int64) {
				duration = durations.Int64[i]
			}
			// Match materializeSpans' timestamp representation, including its signed-bit conversion.
			start := otelstorage.Timestamp(ts).AsTime()
			end := otelstorage.Timestamp(ts + duration).AsTime()
			extent, found := extents[id]
			if !found || start.Before(extent.start) {
				extent.start = start
			}
			if !found || end.After(extent.end) {
				extent.end = end
			}
			extents[id] = extent
		}
		batch.Release()
	}

	keepDuration := traceExtentPredicate(params.TraceDurationMatchers)
	for id := range ids {
		extent, ok := extents[id]
		if !ok || !params.ContainsTrace(extent.start, extent.end) ||
			(q.b.traceQLPushdown && !keepDuration(extent.end.Sub(extent.start))) {
			delete(ids, id)
		}
	}
	return ids, nil
}

func traceExtentPredicate(matchers []traceql.SpanMatcher) func(time.Duration) bool {
	var preds []func(traceql.Static) bool
	for _, m := range matchers {
		if m.Attribute.Prop != traceql.TraceDuration || m.Attribute.Parent || m.Op == 0 {
			continue
		}
		if pred, ok := staticPredicate(m.Op, m.Static); ok {
			preds = append(preds, pred)
		}
	}
	return func(duration time.Duration) bool {
		var value traceql.Static
		value.SetDuration(duration)
		for _, pred := range preds {
			if !pred(value) {
				return false
			}
		}
		return true
	}
}
