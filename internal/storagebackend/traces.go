package storagebackend

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

var (
	_ tracestorage.Querier  = (*TraceQuerier)(nil)
	_ traceqlengine.Querier = (*TraceQuerier)(nil)
)

// SelectSpansets implements [traceqlengine.Querier]. It returns every trace whose spans fall in the
// window, grouped by trace id; the TraceQL engine evaluates the spanset matchers on the result
// (mirroring the in-memory reference querier).
func (q *TraceQuerier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	spans, err := q.scanSpans(ctx, params.Start, params.End)
	if err != nil {
		return nil, err
	}

	order := make([]otelstorage.TraceID, 0)
	byTrace := map[otelstorage.TraceID][]tracestorage.Span{}
	for _, span := range spans {
		if _, ok := byTrace[span.TraceID]; !ok {
			order = append(order, span.TraceID)
		}
		byTrace[span.TraceID] = append(byTrace[span.TraceID], span)
	}

	traces := make([]traceqlengine.Trace, 0, len(order))
	for _, id := range order {
		if params.Limit > 0 && len(traces) >= params.Limit {
			break
		}
		traces = append(traces, traceqlengine.Trace{TraceID: id, Spans: byTrace[id]})
	}
	return iterators.Slice(traces), nil
}

// TraceByID implements [tracestorage.Querier]. It fetches every span of one trace by id.
func (q *TraceQuerier) TraceByID(ctx context.Context, id otelstorage.TraceID, _ tracestorage.TraceByIDOptions) (iterators.Iterator[tracestorage.Span], error) {
	batches, err := q.b.store.Trace(ctx, q.b.tenant, id[:])
	if err != nil {
		return nil, errors.Wrap(err, "trace by id")
	}
	var spans []tracestorage.Span
	for _, batch := range batches {
		spans = append(spans, materializeSpans(batch)...)
	}
	return iterators.Slice(spans), nil
}

// SearchTags implements [tracestorage.Querier]. It returns the spans whose attributes match every
// requested tag and whose duration is within the optional bounds.
func (q *TraceQuerier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (iterators.Iterator[tracestorage.Span], error) {
	spans, err := q.scanSpans(ctx, opts.Start, opts.End)
	if err != nil {
		return nil, err
	}

	var out []tracestorage.Span
	for _, span := range spans {
		if !durationInRange(span, opts.MinDuration, opts.MaxDuration) {
			continue
		}
		if spanMatchesTags(span, tags) {
			out = append(out, span)
		}
	}
	return iterators.Slice(out), nil
}

// TagNames implements [tracestorage.Querier]. It enumerates the distinct attribute names seen on the
// spans in the window, restricted to the requested scope.
func (q *TraceQuerier) TagNames(ctx context.Context, opts tracestorage.TagNamesOptions) ([]tracestorage.TagName, error) {
	spans, err := q.scanSpans(ctx, opts.Start, opts.End)
	if err != nil {
		return nil, err
	}

	seen := map[tracestorage.TagName]struct{}{}
	for _, span := range spans {
		forEachSpanTag(span, func(scope traceql.AttributeScope, name, _ string) {
			if opts.Scope != traceql.ScopeNone && opts.Scope != scope {
				return
			}
			seen[tracestorage.TagName{Scope: scope, Name: name}] = struct{}{}
		})
	}

	out := make([]tracestorage.TagName, 0, len(seen))
	for tn := range seen {
		out = append(out, tn)
	}
	return out, nil
}

// TagValues implements [tracestorage.Querier]. It enumerates the distinct values the attribute takes
// across the spans in the window.
func (q *TraceQuerier) TagValues(ctx context.Context, attr traceql.Attribute, opts tracestorage.TagValuesOptions) (iterators.Iterator[tracestorage.Tag], error) {
	spans, err := q.scanSpans(ctx, opts.Start, opts.End)
	if err != nil {
		return nil, err
	}

	seen := map[string]tracestorage.Tag{}
	for _, span := range spans {
		forEachSpanTag(span, func(scope traceql.AttributeScope, name, value string) {
			if name != attr.Name {
				return
			}
			if attr.Scope != traceql.ScopeNone && attr.Scope != scope {
				return
			}
			seen[value] = tracestorage.Tag{Name: name, Value: value, Type: traceql.TypeString, Scope: scope}
		})
	}

	out := make([]tracestorage.Tag, 0, len(seen))
	for _, tag := range seen {
		out = append(out, tag)
	}
	return iterators.Slice(out), nil
}

// scanSpans fetches and materializes every span in the window.
func (q *TraceQuerier) scanSpans(ctx context.Context, start, end time.Time) ([]tracestorage.Span, error) {
	lo, hi := fetchWindow(start, end)
	req := fetch.Request{
		Tenant: q.b.tenant,
		Signal: signal.Trace,
		Start:  lo,
		End:    hi,
	}

	it, err := q.b.store.TraceFetcher(q.b.tenant).Fetch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch spans")
	}
	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain spans")
	}

	var spans []tracestorage.Span
	for _, batch := range batches {
		spans = append(spans, materializeSpans(batch)...)
	}
	return spans, nil
}

// materializeSpans converts one trace batch into spans, decoding the per-span columns and the
// stream's resource/scope identity.
func materializeSpans(batch *fetch.Batch) []tracestorage.Span {
	bytesCol := func(name string) [][]byte {
		if c, ok := batch.Column(name); ok {
			return c.Bytes
		}
		return nil
	}
	intCol := func(name string) []int64 {
		if c, ok := batch.Column(name); ok {
			return c.Int64
		}
		return nil
	}

	var (
		duration  = intCol(sigtrace.ColDuration)
		kind      = intCol(sigtrace.ColKind)
		status    = intCol(sigtrace.ColStatusCode)
		traceID   = bytesCol(sigtrace.ColTraceID)
		spanID    = bytesCol(sigtrace.ColSpanID)
		parentID  = bytesCol(sigtrace.ColParentSpanID)
		name      = bytesCol(sigtrace.ColName)
		statusMsg = bytesCol(sigtrace.ColStatusMsg)
		attrs     = bytesCol(sigtrace.ColAttrs)
		events    = bytesCol(sigtrace.ColEvents)
		links     = bytesCol(sigtrace.ColLinks)

		resourceAttrs = otelAttrs(batch.Series.Resource.Attributes)
		scopeName     = string(batch.Series.Scope.Name)
		scopeVersion  = string(batch.Series.Scope.Version)
		scopeAttrs    = otelAttrs(batch.Series.Scope.Attributes)
	)

	at := func(s [][]byte, i int) []byte {
		if i < len(s) {
			return s[i]
		}
		return nil
	}
	atInt := func(s []int64, i int) int64 {
		if i < len(s) {
			return s[i]
		}
		return 0
	}

	spans := make([]tracestorage.Span, 0, len(batch.Timestamps))
	for i := range batch.Timestamps {
		start := batch.Timestamps[i]
		span := tracestorage.Span{
			TraceID:       otelTraceID(at(traceID, i)),
			SpanID:        otelSpanID(at(spanID, i)),
			ParentSpanID:  otelSpanID(at(parentID, i)),
			Name:          string(at(name, i)),
			Kind:          int32(atInt(kind, i)),
			Start:         otelstorage.Timestamp(start),
			End:           otelstorage.Timestamp(start + atInt(duration, i)),
			StatusCode:    int32(atInt(status, i)),
			StatusMessage: string(at(statusMsg, i)),
			ResourceAttrs: resourceAttrs,
			ScopeName:     scopeName,
			ScopeVersion:  scopeVersion,
			ScopeAttrs:    scopeAttrs,
		}
		if raw := at(attrs, i); len(raw) > 0 {
			if decoded, _, err := signal.DecodeAttributes(raw); err == nil {
				span.Attrs = otelAttrs(decoded)
			}
		}
		if raw := at(events, i); len(raw) > 0 {
			if decoded, err := sigtrace.DecodeEvents(raw); err == nil {
				span.Events = convertEvents(decoded)
			}
		}
		if raw := at(links, i); len(raw) > 0 {
			if decoded, err := sigtrace.DecodeLinks(raw); err == nil {
				span.Links = convertLinks(decoded)
			}
		}
		spans = append(spans, span)
	}
	return spans
}

// convertEvents maps storage span events to tracestorage events.
func convertEvents(evs []sigtrace.Event) []tracestorage.Event {
	if len(evs) == 0 {
		return nil
	}
	out := make([]tracestorage.Event, len(evs))
	for i, e := range evs {
		out[i] = tracestorage.Event{
			Timestamp: otelstorage.Timestamp(e.Time),
			Name:      string(e.Name),
			Attrs:     otelAttrs(e.Attributes),
		}
	}
	return out
}

// convertLinks maps storage span links to tracestorage links.
func convertLinks(ls []sigtrace.Link) []tracestorage.Link {
	if len(ls) == 0 {
		return nil
	}
	out := make([]tracestorage.Link, len(ls))
	for i, l := range ls {
		out[i] = tracestorage.Link{
			TraceID:    otelTraceID(l.TraceID),
			SpanID:     otelSpanID(l.SpanID),
			TraceState: string(l.TraceState),
			Attrs:      otelAttrs(l.Attributes),
		}
	}
	return out
}

// durationInRange reports whether the span's duration is within the optional [min, max] bounds.
func durationInRange(span tracestorage.Span, min, max time.Duration) bool {
	d := time.Duration(span.End - span.Start)
	if min > 0 && d < min {
		return false
	}
	if max > 0 && d > max {
		return false
	}
	return true
}

// spanMatchesTags reports whether the span carries every requested tag with the requested value.
func spanMatchesTags(span tracestorage.Span, tags map[string]string) bool {
	for k, want := range tags {
		got, ok := lookupSpanTag(span, k)
		if !ok || got != want {
			return false
		}
	}
	return true
}

// lookupSpanTag returns the string value of a tag on the span, searching the intrinsic name, then
// span, resource, and scope attributes.
func lookupSpanTag(span tracestorage.Span, name string) (string, bool) {
	if name == "name" {
		return span.Name, span.Name != ""
	}
	for _, attrs := range []otelstorage.Attrs{span.Attrs, span.ResourceAttrs, span.ScopeAttrs} {
		if attrs.IsZero() {
			continue
		}
		if v, ok := attrs.AsMap().Get(name); ok {
			return v.AsString(), true
		}
	}
	return "", false
}

// forEachSpanTag calls fn for every attribute of the span, tagged with its scope.
func forEachSpanTag(span tracestorage.Span, fn func(scope traceql.AttributeScope, name, value string)) {
	visit := func(scope traceql.AttributeScope, attrs otelstorage.Attrs) {
		if attrs.IsZero() {
			return
		}
		attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
			fn(scope, k, v.AsString())
			return true
		})
	}
	visit(traceql.ScopeResource, span.ResourceAttrs)
	visit(traceql.ScopeInstrumentation, span.ScopeAttrs)
	visit(traceql.ScopeSpan, span.Attrs)
}
