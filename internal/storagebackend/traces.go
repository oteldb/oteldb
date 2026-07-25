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

// SelectSpansets implements [traceqlengine.Querier]. It returns the traces whose spans fall in the
// window, grouped by trace id; the TraceQL engine evaluates the spanset matchers on the result.
//
// The query's span matchers are first lowered to storage filters ([buildTracePushdown]) and run as a
// trace_id-only scan, so only the candidate traces are materialized with their attributes, events
// and links. The candidate set is a superset of the result and whole traces are returned, so the
// engine sees exactly what a full window scan would give it — structural operators and the
// spanset-level intrinsics (rootName, traceDuration) still work. Nothing pushable (a bare `{}`, a
// root-intrinsic query) falls back to the full window scan.
//
// Resolving the candidates is itself a scan (of the filter columns and trace_id, not of the
// attribute/event/link blobs), so a predicate that matches nearly every trace pays for it without
// pruning anything: on the golden corpus a selective query is ~4x faster and a match-everything one
// ~18% slower than the plain scan.
//
// params.Limit bounds the candidate traces only when the query is provably exact on both sides —
// see [TraceQuerier.limitApplies]. It is never applied to the traces this returns: the engine
// evaluates the expression and applies the limit to the *matches*, so truncating a candidate set
// that is a superset would under-report them.
func (q *TraceQuerier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	traceIDs, pd, err := q.candidateTraces(ctx, params)
	if err != nil {
		return nil, err
	}
	if pd.pushed && len(traceIDs) == 0 {
		return iterators.Empty[traceqlengine.Trace](), nil
	}

	if q.limitApplies(params, pd, len(traceIDs)) {
		traceIDs, err = q.boundCandidates(ctx, params, traceIDs)
		if err != nil {
			return nil, err
		}
	}

	spans, err := q.scanSpans(ctx, params.Start, params.End, traceIDs)
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
		traces = append(traces, traceqlengine.Trace{TraceID: id, Spans: byTrace[id]})
	}
	return iterators.Slice(traces), nil
}

// limitApplies reports whether params.Limit may bound the candidate traces.
//
// It needs both halves of the exactness argument to hold, and fails closed on either:
//
//   - the engine's, that the matcher list is the whole query, so a trace holding one span that
//     satisfies every matcher is a result ([traceqlengine.SelectSpansetsParams.Exact]);
//   - the pushdown's, that the candidate traces are exactly those traces and not a superset
//     ([tracePushdown.exact]).
//
// Without both, N candidates are not N results and bounding them would under-report matches — the
// bug the plain scan had. Bounding a candidate set no larger than the limit is a no-op, so it is
// skipped: the caller then never pays for [TraceQuerier.boundCandidates]. [WithTraceQLLimitPushdown]
// turns it off wholesale.
func (q *TraceQuerier) limitApplies(params traceqlengine.SelectSpansetsParams, pd tracePushdown, candidates int) bool {
	return q.b.traceQLLimitPushdown &&
		pd.pushed && pd.exact && params.Exact &&
		params.Limit > 0 && candidates >= params.Limit*traceLimitBoundFactor
}

// traceLimitBoundFactor is how much larger than the limit the candidate set must be for bounding it
// to pay for itself. [TraceQuerier.boundCandidates] costs one extra scan of the candidates' cheap
// columns and saves materializing all but the limit of them, so a candidate set only slightly above
// the limit comes out behind. On the golden corpus the crossover is between 2.5x (a ~13% loss) and
// 25x (a ~60% win); 4 sits past it. Purely a cost heuristic — bounding is invisible either way.
const traceLimitBoundFactor = 4

// boundCandidates narrows the candidates to the first limit traces [TraceQuerier.scanSpans] would
// yield, so only those are materialized with their attributes, events and links.
//
// It resolves that prefix with a second fetch over the candidates that projects only the timestamp
// and duration columns — no attribute, event or link blob is decoded. That fetch is the *same
// request* as the materializing one but for the projection, so it visits the traces in the same
// order, and a trace's bounds over it are the bounds the engine computes over the materialized
// spans. Restricting the materializing fetch to a subset of the same candidates preserves that
// order, so the traces the engine sees are exactly the ones it would have kept unbounded.
//
// The engine drops a trace that is not wholly inside the window ([traceqlengine.timeRange]), which
// a fetch does not: a span starting inside it may end after it. That check is reproduced here so a
// dropped trace does not consume one of the limit's slots.
func (q *TraceQuerier) boundCandidates(
	ctx context.Context, params traceqlengine.SelectSpansetsParams, candidates map[otelstorage.TraceID]struct{},
) (map[otelstorage.TraceID]struct{}, error) {
	lo, hi := fetchWindow(params.Start, params.End)
	it, err := q.b.store.TraceFetcher(q.b.tenant).Fetch(ctx, fetch.Request{
		Tenant:        q.b.tenant,
		Signal:        signal.Trace,
		Start:         lo,
		End:           hi,
		Conditions:    []fetch.Condition{traceIDCondition(candidates)},
		AllConditions: true,
		Projection:    []string{sigtrace.ColTraceID, sigtrace.ColDuration},
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch candidate bounds")
	}
	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain candidate bounds")
	}

	type bounds struct{ start, end int64 }
	var (
		order  []otelstorage.TraceID
		byID   = map[otelstorage.TraceID]bounds{}
		zeroID otelstorage.TraceID
	)
	for _, batch := range batches {
		idCol, ok := batch.Column(sigtrace.ColTraceID)
		if !ok {
			continue
		}
		var durations []int64
		if c, ok := batch.Column(sigtrace.ColDuration); ok {
			durations = c.Int64
		}
		for i, start := range batch.Timestamps {
			id := zeroID
			if i < len(idCol.Bytes) {
				id = otelTraceID(idCol.Bytes[i])
			}
			end := start
			if i < len(durations) {
				end += durations[i]
			}

			b, seen := byID[id]
			if !seen {
				order = append(order, id)
				b = bounds{start: start, end: end}
			}
			b.start = min(b.start, start)
			b.end = max(b.end, end)
			byID[id] = b
		}
	}

	out := make(map[otelstorage.TraceID]struct{}, params.Limit)
	for _, id := range order {
		if len(out) >= params.Limit {
			break
		}
		if !traceWithin(byID[id].start, byID[id].end, params.Start, params.End) {
			continue
		}
		out[id] = struct{}{}
	}
	return out, nil
}

// traceWithin mirrors the engine's per-trace window check ([traceqlengine.timeRange.within]) over a
// trace's unix-nanosecond bounds. The duration half of that check is not reproduced: a trace
// duration bound makes the query inexact, so the limit is never bounded when one is set.
func traceWithin(start, end int64, from, to time.Time) bool {
	if !from.IsZero() && start < from.UnixNano() {
		return false
	}
	if !to.IsZero() && end > to.UnixNano() {
		return false
	}
	return true
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
	spans, err := q.scanSpans(ctx, opts.Start, opts.End, nil)
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
	spans, err := q.scanSpans(ctx, opts.Start, opts.End, nil)
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
	spans, err := q.scanSpans(ctx, opts.Start, opts.End, nil)
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

// candidateTraces resolves the query's span matchers to storage filters and returns the ids of the
// traces holding at least one span that can match, with the pushdown that selected them. A zero
// pushdown means nothing was pushed: the ids are nil and the caller scans the whole window.
//
// A union of matchers needs one fetch per branch, since conditions AND within a request.
func (q *TraceQuerier) candidateTraces(
	ctx context.Context, params traceqlengine.SelectSpansetsParams,
) (_ map[otelstorage.TraceID]struct{}, _ tracePushdown, _ error) {
	if !q.b.traceQLPushdown {
		return nil, tracePushdown{}, nil
	}
	pd, ok := buildTracePushdown(params.Op, params.Matchers)
	if !ok {
		return nil, tracePushdown{}, nil
	}
	pd.pushed = true

	lo, hi := fetchWindow(params.Start, params.End)
	ids := map[otelstorage.TraceID]struct{}{}
	for _, group := range pd.groups {
		req := fetch.Request{
			Tenant:   q.b.tenant,
			Signal:   signal.Trace,
			Start:    lo,
			End:      hi,
			Matchers: group.matchers,
			// Only the trace id of a surviving span is needed; the filter columns are decoded
			// regardless, but the attrs/events/links blobs are not.
			Projection: []string{sigtrace.ColTraceID},
		}
		if len(group.conditions) > 0 {
			req.Conditions = group.conditions
			req.AllConditions = true
		}

		it, err := q.b.store.TraceFetcher(q.b.tenant).Fetch(ctx, req)
		if err != nil {
			return nil, tracePushdown{}, errors.Wrap(err, "fetch candidate traces")
		}
		batches, err := fetch.Drain(ctx, it)
		if err != nil {
			return nil, tracePushdown{}, errors.Wrap(err, "drain candidate traces")
		}
		for _, batch := range batches {
			col, ok := batch.Column(sigtrace.ColTraceID)
			if !ok {
				continue
			}
			for _, raw := range col.Bytes {
				ids[otelTraceID(raw)] = struct{}{}
			}
		}
	}
	return ids, pd, nil
}

// scanSpans fetches and materializes the spans in the window. A non-nil traceIDs restricts the scan
// to those traces, so the spans of every other trace are never materialized.
func (q *TraceQuerier) scanSpans(
	ctx context.Context, start, end time.Time, traceIDs map[otelstorage.TraceID]struct{},
) ([]tracestorage.Span, error) {
	lo, hi := fetchWindow(start, end)
	req := fetch.Request{
		Tenant: q.b.tenant,
		Signal: signal.Trace,
		Start:  lo,
		End:    hi,
	}
	if traceIDs != nil {
		req.Conditions = []fetch.Condition{traceIDCondition(traceIDs)}
		req.AllConditions = true
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

// traceIDCondition builds the per-span condition keeping only the spans of the given traces. A
// single id also carries the equality-bloom hint, so a trace-by-id shaped query prunes to the parts
// that hold it.
func traceIDCondition(traceIDs map[otelstorage.TraceID]struct{}) fetch.Condition {
	cond := fetch.Condition{
		Column: sigtrace.ColTraceID,
		Match: func(v signal.Value) bool {
			raw := v.Str()
			if len(raw) != len(otelstorage.TraceID{}) {
				return false
			}
			_, ok := traceIDs[otelTraceID(raw)]
			return ok
		},
	}
	if len(traceIDs) == 1 {
		for id := range traceIDs {
			cond.Equal = &fetch.EqualMatcher{Name: sigtrace.ColTraceID, Value: string(id[:])}
		}
	}
	return cond
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

// durationInRange reports whether the span's duration is within the optional [minDur, maxDur]
// bounds.
func durationInRange(span tracestorage.Span, minDur, maxDur time.Duration) bool {
	d := time.Duration(span.End - span.Start)
	if minDur > 0 && d < minDur {
		return false
	}
	if maxDur > 0 && d > maxDur {
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
