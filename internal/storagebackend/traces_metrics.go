package storagebackend

import (
	"context"
	"io"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

var _ traceqlengine.MetricsQuerier = (*TraceQuerier)(nil)

// WithQueryBudget shares native read admission and memory accounting across metrics operands.
func (q *TraceQuerier) WithQueryBudget(ctx context.Context) context.Context {
	return q.b.queryContext(ctx)
}

// SelectMetricsSpansets fetches complete candidate traces; the metrics source
// applies span-start buckets after structural evaluation, rather than search containment.
func (q *TraceQuerier) SelectMetricsSpansets(ctx context.Context, p traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	return q.selectSpansets(ctx, p, false)
}

// SelectMetricsSpans streams projected spans in the inclusive start-time window.
// The source rechecks the full expression; index filters remain optional optimizations.
func (q *TraceQuerier) SelectMetricsSpans(ctx context.Context, p traceqlengine.SelectSpansetsParams) (iterators.Iterator[tracestorage.Span], error) {
	ctx = q.b.queryContext(ctx)
	lo, hi := fetchWindow(p.Start, p.End)
	request := fetch.Request{Tenant: q.b.tenant, Signal: signal.Trace, Start: lo, End: hi, Projection: traceMetricsProjection(p.Fields)}
	// Only use a single necessary conjunction. Disjunctions and split terms stay residual.
	if q.b.traceQLPushdown {
		if pd, ok := buildTracePushdown(p.Op, p.Matchers); ok && len(pd.terms) == 1 && len(pd.terms[0].groups) == 1 {
			group := pd.terms[0].groups[0]
			request.Matchers, request.Conditions = group.matchers, group.conditions
			request.AllConditions = true
		}
	}
	input, err := q.b.src.TraceFetcher(q.b.tenant).Fetch(ctx, request)
	if err != nil {
		return nil, err
	}
	return &metricSpanIterator{ctx: ctx, input: input}, nil
}

func traceMetricsProjection(fields []traceql.Attribute) []string {
	// A nonempty projection avoids the all-columns default; timestamps and stream
	// identity are implicit. No attrs/name/duration blobs are needed for a bare count.
	columns := []string{sigtrace.ColTraceID}
	add := func(name string) {
		if !slices.Contains(columns, name) {
			columns = append(columns, name)
		}
	}
	for _, field := range fields {
		switch field.Prop {
		case traceql.SpanAttribute:
			switch field.Scope {
			case traceql.ScopeNone, traceql.ScopeSpan:
				add(sigtrace.ColAttrs)
			case traceql.ScopeEvent:
				add(sigtrace.ColEvents)
			case traceql.ScopeLink:
				add(sigtrace.ColLinks)
			}
		case traceql.SpanDuration:
			add(sigtrace.ColDuration)
		case traceql.SpanName:
			add(sigtrace.ColName)
		case traceql.SpanKind:
			add(sigtrace.ColKind)
		case traceql.SpanStatus:
			add(sigtrace.ColStatusCode)
		case traceql.SpanStatusMessage:
			add(sigtrace.ColStatusMsg)
		case traceql.SpanID:
			add(sigtrace.ColSpanID)
		case traceql.ParentID:
			add(sigtrace.ColParentSpanID)
		case traceql.EventName, traceql.EventTimeSinceStart:
			add(sigtrace.ColEvents)
		case traceql.LinkTraceID, traceql.LinkSpanID:
			add(sigtrace.ColLinks)
		}
	}
	return columns
}

type metricSpanIterator struct {
	ctx    context.Context
	input  fetch.Iterator
	spans  []tracestorage.Span
	next   int
	err    error
	closed bool
}

func (i *metricSpanIterator) Next(out *tracestorage.Span) bool {
	if i.closed || i.err != nil {
		return false
	}
	for i.next == len(i.spans) {
		if err := i.ctx.Err(); err != nil {
			i.err = err
			return false
		}
		batch, err := i.input.Next(i.ctx)
		if errors.Is(err, io.EOF) {
			return false
		}
		if err != nil {
			i.err = err
			return false
		}
		if batch == nil {
			return false
		}
		i.spans, i.next = materializeSpans(batch), 0
		batch.Release()
	}
	*out = i.spans[i.next]
	i.next++
	return true
}
func (i *metricSpanIterator) Err() error { return i.err }
func (i *metricSpanIterator) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	i.spans = nil
	return i.input.Close()
}
