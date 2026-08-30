package storagebackend

import (
	"context"
	"maps"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage"
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
// spanset-level intrinsics still work. Nothing pushable (a bare `{}`, a `traceDuration` bound)
// falls back to the full window scan.
//
// Resolving the candidates is itself a scan (of the filter columns and trace_id, not of the
// attribute/event/link blobs), so a predicate that matches nearly every trace pays for it without
// pruning anything: on the golden corpus a selective query is ~4x faster and a match-everything one
// ~18% slower than the plain scan.
//
// params.Limit is deliberately not applied here: it counts *matching* traces, and the candidate set
// is only a superset of them. Truncating it in scan order would cap the result at "however many of
// the first N candidates happen to match", which for a selective query is usually fewer than N and
// often zero. The engine applies the limit once the matchers have run.
func (q *TraceQuerier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	ctx = queryScope(ctx)

	traceIDs, pushed, err := q.candidateTraces(ctx, params)
	if err != nil {
		return nil, err
	}
	if pushed && len(traceIDs) == 0 {
		return iterators.Empty[traceqlengine.Trace](), nil
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

// TraceByID implements [tracestorage.Querier]. It fetches every span of one trace by id.
func (q *TraceQuerier) TraceByID(ctx context.Context, id otelstorage.TraceID, _ tracestorage.TraceByIDOptions) (iterators.Iterator[tracestorage.Span], error) {
	batches, err := q.b.src.Trace(ctx, q.b.tenant, id[:])
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
//
// Only the string-ish intrinsics are enumerable: name, status, kind, rootName and rootServiceName.
// duration, traceDuration and childCount are numeric/unbounded and parent is structural, so — mirroring
// Tempo and [chstorage.Querier.TagValues] — they yield no autocomplete values.
//
// Two shapes are answered from the parts' column dictionaries via [storagebackend.Source.ColumnValues]
// instead of a full window scan: the name intrinsic ([sigtrace.ColName]) and a span-scoped attribute
// (attr.Scope == [traceql.ScopeSpan]). Both are O(distinct values); the result is a superset of the
// window (a part overlapping it contributes its whole dictionary), which the storage doc calls out as
// fine for autocomplete and this package already relies on elsewhere.
//
// Everything else still scans:
//   - rootName and rootServiceName filter to root spans (empty parent span id), a predicate a column
//     enumeration cannot express.
//   - a resource- or instrumentation-scoped attribute lives on the stream identity, not the per-record
//     attribute blob AttrKey enumerates, and there is no trace-stream enumeration primitive on [Source]
//     (unlike [LogQuerier], which has LogSeries) to answer it more cheaply.
//   - ScopeNone must cover every scope, so it needs both the span-scoped and the stream-scoped values;
//     since only the former is pushable, the whole lookup falls back to the scan rather than silently
//     dropping resource/instrumentation values.
func (q *TraceQuerier) TagValues(ctx context.Context, attr traceql.Attribute, opts tracestorage.TagValuesOptions) (iterators.Iterator[tracestorage.Tag], error) {
	switch attr.Prop {
	case traceql.SpanStatus:
		return iterators.Slice(spanStatusTags(attr)), nil
	case traceql.SpanKind:
		return iterators.Slice(spanKindTags(attr)), nil
	case traceql.SpanDuration, traceql.SpanChildCount, traceql.SpanParent, traceql.TraceDuration:
		return iterators.Empty[tracestorage.Tag](), nil
	case traceql.SpanName:
		return q.columnTagValues(ctx, attr, sigtrace.ColName, opts)
	case traceql.SpanAttribute:
		if attr.Scope == traceql.ScopeSpan {
			return q.attrTagValues(ctx, attr, opts)
		}
	}

	spans, err := q.scanSpans(ctx, opts.Start, opts.End, nil)
	if err != nil {
		return nil, err
	}

	switch attr.Prop {
	case traceql.RootSpanName:
		return iterators.Slice(spanNameTags(attr, spans)), nil
	case traceql.RootServiceName:
		return iterators.Slice(rootServiceNameTags(attr, spans)), nil
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

// columnTagValues enumerates a byte column's distinct values via [Source.ColumnValues].
func (q *TraceQuerier) columnTagValues(
	ctx context.Context, attr traceql.Attribute, column string, opts tracestorage.TagValuesOptions,
) (iterators.Iterator[tracestorage.Tag], error) {
	lo, hi := seriesWindow(opts.Start, opts.End)
	values, err := q.b.src.ColumnValues(ctx, q.b.tenant, storage.ValuesRequest{
		Signal: signal.Trace,
		Column: column,
		Start:  lo,
		End:    hi,
	})
	if err != nil {
		return nil, errors.Wrap(err, "column values")
	}

	name := attr.String()
	out := make([]tracestorage.Tag, 0, len(values))
	for _, v := range values {
		out = append(out, tracestorage.Tag{Name: name, Value: string(v), Type: traceql.TypeString})
	}
	return iterators.Slice(out), nil
}

// attrTagValues enumerates a span-scoped attribute's distinct values via [Source.ColumnValues]. It
// must not be called for any other scope: resource and instrumentation attributes are not part of
// the per-record attribute blob AttrKey enumerates.
func (q *TraceQuerier) attrTagValues(
	ctx context.Context, attr traceql.Attribute, opts tracestorage.TagValuesOptions,
) (iterators.Iterator[tracestorage.Tag], error) {
	lo, hi := seriesWindow(opts.Start, opts.End)
	values, err := q.b.src.ColumnValues(ctx, q.b.tenant, storage.ValuesRequest{
		Signal:  signal.Trace,
		AttrKey: []byte(attr.Name),
		Start:   lo,
		End:     hi,
	})
	if err != nil {
		return nil, errors.Wrap(err, "column values")
	}

	name := attr.String()
	out := make([]tracestorage.Tag, 0, len(values))
	for _, v := range values {
		out = append(out, tracestorage.Tag{Name: name, Value: string(v), Type: traceql.TypeString, Scope: traceql.ScopeSpan})
	}
	return iterators.Slice(out), nil
}

// spanStatusTags returns the full [traceql.TypeSpanStatus] enum, formatted the way TraceQL expects
// (see [SpanMatcher.String]), regardless of which statuses are actually present in the window.
func spanStatusTags(attr traceql.Attribute) []tracestorage.Tag {
	name := attr.String()
	values := [...]string{"unset", "ok", "error"}
	out := make([]tracestorage.Tag, 0, len(values))
	for _, v := range values {
		out = append(out, tracestorage.Tag{Name: name, Value: v, Type: traceql.TypeSpanStatus})
	}
	return out
}

// spanKindTags returns the full [traceql.TypeSpanKind] enum, formatted the way TraceQL expects (see
// [SpanMatcher.String]), regardless of which kinds are actually present in the window.
func spanKindTags(attr traceql.Attribute) []tracestorage.Tag {
	name := attr.String()
	values := [...]string{"unspecified", "internal", "server", "client", "producer", "consumer"}
	out := make([]tracestorage.Tag, 0, len(values))
	for _, v := range values {
		out = append(out, tracestorage.Tag{Name: name, Value: v, Type: traceql.TypeSpanKind})
	}
	return out
}

// spanNameTags enumerates the distinct span names in spans. For RootSpanName it is restricted to
// root spans (empty parent span id), since a root span's name is a span-local property and needs no
// full trace assembly.
func spanNameTags(attr traceql.Attribute, spans []tracestorage.Span) []tracestorage.Tag {
	name := attr.String()
	seen := map[string]struct{}{}
	var out []tracestorage.Tag
	for _, span := range spans {
		if attr.Prop == traceql.RootSpanName && !span.ParentSpanID.IsEmpty() {
			continue
		}
		if span.Name == "" {
			continue
		}
		if _, ok := seen[span.Name]; ok {
			continue
		}
		seen[span.Name] = struct{}{}
		out = append(out, tracestorage.Tag{Name: name, Value: span.Name, Type: traceql.TypeString})
	}
	return out
}

// rootServiceNameTags enumerates the distinct service.name resource attribute values of root spans
// (empty parent span id). A root span's own resource is a span-local property, so this needs no full
// trace assembly.
func rootServiceNameTags(attr traceql.Attribute, spans []tracestorage.Span) []tracestorage.Tag {
	name := attr.String()
	seen := map[string]struct{}{}
	var out []tracestorage.Tag
	for _, span := range spans {
		if !span.ParentSpanID.IsEmpty() {
			continue
		}
		svc, ok := span.ServiceName()
		if !ok || svc == "" {
			continue
		}
		if _, ok := seen[svc]; ok {
			continue
		}
		seen[svc] = struct{}{}
		out = append(out, tracestorage.Tag{Name: name, Value: svc, Type: traceql.TypeString})
	}
	return out
}

// candidateTraces resolves the query's span matchers to storage filters and returns the ids of the
// traces holding at least one span that can match. pushed reports whether any filter was pushed at
// all; when it is false the ids are nil and the caller scans the whole window.
//
// Each of the pushdown's terms is resolved to its own id set — one fetch per group, unioned, since
// conditions AND within a request — and the terms then intersect.
func (q *TraceQuerier) candidateTraces(
	ctx context.Context, params traceqlengine.SelectSpansetsParams,
) (_ map[otelstorage.TraceID]struct{}, pushed bool, _ error) {
	if !q.b.traceQLPushdown {
		return nil, false, nil
	}
	pd, ok := buildTracePushdown(params.Op, params.Matchers)
	if !ok {
		return nil, false, nil
	}

	lo, hi := fetchWindow(params.Start, params.End)

	var ids map[otelstorage.TraceID]struct{}
	for _, term := range pd.terms {
		got := map[otelstorage.TraceID]struct{}{}
		for _, group := range term.groups {
			if err := q.collectTraceIDs(ctx, lo, hi, group, got); err != nil {
				return nil, false, err
			}
		}

		if ids == nil {
			ids = got
		} else {
			maps.DeleteFunc(ids, func(id otelstorage.TraceID, _ struct{}) bool {
				_, ok := got[id]
				return !ok
			})
		}
		if len(ids) == 0 {
			break
		}
	}
	return ids, true, nil
}

// collectTraceIDs runs one filter group over the window and adds the trace id of every surviving
// span to ids.
func (q *TraceQuerier) collectTraceIDs(
	ctx context.Context, lo, hi int64, group traceFilter, ids map[otelstorage.TraceID]struct{},
) error {
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

	it, err := q.b.src.TraceFetcher(q.b.tenant).Fetch(ctx, req)
	if err != nil {
		return errors.Wrap(err, "fetch candidate traces")
	}
	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return errors.Wrap(err, "drain candidate traces")
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
	return nil
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

	it, err := q.b.src.TraceFetcher(q.b.tenant).Fetch(ctx, req)
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
