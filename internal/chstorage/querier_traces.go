package chstorage

import (
	"context"
	"encoding/hex"
	"iter"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// SearchTags performs search by given tags.
func (q *Querier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.SearchTags",
		trace.WithAttributes(
			xattribute.StringMap("chstorage.tags", tags),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			xattribute.Duration("chstorage.min_duration", opts.MinDuration),
			xattribute.Duration("chstorage.max_duration", opts.MaxDuration),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	subquery := chsql.Select(table, chsql.Column("trace_id", nil)).
		Distinct(true).
		Where(traceInTimeRange(opts.Start, opts.End))
	{
		durationExpr := chsql.Ident("duration_ns")
		if d := opts.MinDuration; d != 0 {
			subquery.Where(
				chsql.Gte(durationExpr, chsql.Integer(int64(d))),
			)
		}
		if d := opts.MaxDuration; d != 0 {
			subquery.Where(
				chsql.Lte(durationExpr, chsql.Integer(int64(d))),
			)
		}
	}
	for key, value := range tags {
		if key == "name" {
			subquery.Where(
				chsql.ColumnEq("name", value),
			)
			continue
		}

		exprs := make([]chsql.Expr, 0, 3)
		for _, column := range []string{
			colAttrs,
			colResource,
			colScope,
		} {
			exprs = append(exprs, chsql.Eq(
				attrSelector(column, key),
				chsql.String(value),
			))
		}
		subquery.Where(chsql.JoinOr(exprs...))
	}

	var (
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(chsql.In(
				chsql.Ident("trace_id"),
				chsql.SubQuery(subquery),
			))

		r []tracestorage.Span
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) (err error) {
			r, err = c.ReadRowsTo(r)
			return err
		},

		Type:   "SearchTags",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

// TagNames returns all available tag names.
func (q *Querier) TagNames(ctx context.Context, opts tracestorage.TagNamesOptions) (r []tracestorage.TagName, rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagNames",
		trace.WithAttributes(
			attribute.Stringer("chstorage.scope", opts.Scope),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		name  = new(proto.ColStr).LowCardinality()
		scope proto.ColEnum8

		query = chsql.Select(table,
			chsql.Column("name", name),
			chsql.Column("scope", &scope),
		).
			Distinct(true)
	)
	switch scope := opts.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.TupleValues(int(scope), int(traceql.ScopeInstrumentation)),
			),
		)
	case traceql.ScopeSpan:
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.Integer(int(scope)),
			),
		)
	default:
		return r, errors.Errorf("unexpected scope %v", scope)
	}

	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				r = append(r, tracestorage.TagName{
					Name:  name.Row(i),
					Scope: traceql.AttributeScope(scope.Row(i)),
				})
			}
			return nil
		},

		Type:   "TagNames",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// TagValues returns all available tag values for given tag.
func (q *Querier) TagValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TagValues",
		trace.WithAttributes(
			attribute.Stringer("chstorage.tag", tag),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.Stringer("traceql.autocomplete", opts.AutocompleteQuery),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	switch tag.Prop {
	case traceql.SpanAttribute:
		return q.attributeValues(ctx, tag, opts)
	case traceql.SpanStatus:
		// TODO(tdakkota): probably we should do a proper query.
		name := tag.String()
		statuses := []tracestorage.Tag{
			{Name: name, Value: "unset", Type: traceql.TypeSpanStatus},
			{Name: name, Value: "ok", Type: traceql.TypeSpanStatus},
			{Name: name, Value: "error", Type: traceql.TypeSpanStatus},
		}
		return iterators.Slice(statuses), nil
	case traceql.SpanKind:
		// TODO(tdakkota): probably we should do a proper query.
		name := tag.String()
		kinds := []tracestorage.Tag{
			{Name: name, Value: "unspecified", Type: traceql.TypeSpanKind},
			{Name: name, Value: "internal", Type: traceql.TypeSpanKind},
			{Name: name, Value: "server", Type: traceql.TypeSpanKind},
			{Name: name, Value: "client", Type: traceql.TypeSpanKind},
			{Name: name, Value: "producer", Type: traceql.TypeSpanKind},
			{Name: name, Value: "consumer", Type: traceql.TypeSpanKind},
		}
		return iterators.Slice(kinds), nil
	case traceql.SpanDuration, traceql.SpanChildCount, traceql.SpanParent, traceql.TraceDuration:
		// Too high cardinality to query.
		return iterators.Empty[tracestorage.Tag](), nil
	case traceql.SpanName, traceql.RootSpanName:
		return q.spanNames(ctx, tag, opts)
	case traceql.RootServiceName:
		// FIXME(tdakkota): we don't check if service.name actually coming from a root span.
		//
		// Equals to `resource.service.name`.
		tag = traceql.Attribute{Name: "service.name", Scope: traceql.ScopeResource}
		return q.attributeValues(ctx, tag, opts)
	default:
		return nil, errors.Errorf("unexpected span property %v (attribute: %q)", tag.Prop, tag)
	}
}

var zeroSpanIDHex = hex.EncodeToString(new(pcommon.SpanID)[:])

func (q *Querier) spanNames(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.spanNames",
		trace.WithAttributes(
			attribute.Stringer("chstorage.tag", tag),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.Stringer("traceql.autocomplete", opts.AutocompleteQuery),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		name  = new(proto.ColStr).LowCardinality()
		query = chsql.Select(table, chsql.Column("name", name)).
			// Select deduplicated column by using GROUP BY, since DISTINCT is not optimized by Clickhouse.
			//
			// See https://github.com/ClickHouse/ClickHouse/issues/4670
			Distinct(true).GroupBy(chsql.Ident("name")).
			Where(traceInTimeRange(opts.Start, opts.End))
	)
	{
		if tag.Prop == traceql.RootSpanName {
			query.Where(spanIsRoot())
		}
		for _, m := range opts.AutocompleteQuery.Matchers {
			e, ok := getTraceQLMatcher(m)
			if !ok {
				continue
			}
			query.Where(e)
		}
	}

	var (
		tagName = tag.String()
		r       []tracestorage.Tag
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				r = append(r, tracestorage.Tag{
					Name:  tagName,
					Value: name.Row(i),
					Type:  traceql.TypeString,
					Scope: traceql.ScopeNone,
				})
			}
			return nil
		},

		Type:   "SpanNames",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

func (q *Querier) attributeValues(ctx context.Context, tag traceql.Attribute, opts tracestorage.TagValuesOptions) (_ iterators.Iterator[tracestorage.Tag], rerr error) {
	table := q.tables.Tags

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.attributeValues",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.Stringer("chstorage.tag", tag),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// FIXME(tdakkota): respect time range parameters.
	var (
		value     proto.ColStr
		valueType proto.ColEnum8

		query = chsql.Select(table,
			chsql.Column("value", &value),
			chsql.Column("value_type", proto.Wrap(&valueType, valueTypeDDL)),
		).
			Distinct(true).
			Where(chsql.ColumnEq("name", tag.Name))
	)
	switch scope := tag.Scope; scope {
	case traceql.ScopeNone:
	case traceql.ScopeResource:
		// Tempo merges scope attributes and resource attributes.
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.TupleValues(int(scope), int(traceql.ScopeInstrumentation)),
			),
		)
	case traceql.ScopeSpan:
		query.Where(
			chsql.In(
				chsql.Ident("scope"),
				chsql.Integer(int(scope)),
			),
		)
	default:
		return nil, errors.Errorf("unexpected scope %v", scope)
	}

	var r []tracestorage.Tag
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			return value.ForEach(func(i int, value string) error {
				typ := pcommon.ValueType(valueType.Row(i))
				r = append(r, tracestorage.Tag{
					Name:  tag.Name,
					Value: value,
					Type:  traceql.StaticTypeFromValueType(typ),
					Scope: traceql.ScopeNone,
				})
				return nil
			})
		},

		Type:   "attributeValues",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

// TraceByID returns spans of given trace.
func (q *Querier) TraceByID(ctx context.Context, id otelstorage.TraceID, opts tracestorage.TraceByIDOptions) (_ iterators.Iterator[tracestorage.Span], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.TraceByID",
		trace.WithAttributes(
			attribute.String("chstorage.id_to_query", id.Hex()),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.Eq(
					chsql.Ident("trace_id"),
					chsql.Unhex(chsql.String(id.Hex())),
				),
				traceInTimeRange(opts.Start, opts.End),
			)
	)

	var r []tracestorage.Span
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) (err error) {
			r, err = c.ReadRowsTo(r)
			return err
		},

		Type:   "TraceByID",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(r), nil
}

var _ traceqlengine.Querier = (*Querier)(nil)

// SelectSpansets get spansets from storage.
func (q *Querier) SelectSpansets(ctx context.Context, params traceqlengine.SelectSpansetsParams) (_ iterators.Iterator[traceqlengine.Trace], rerr error) {
	table := q.tables.Spans

	ctx, span := q.tracer.Start(ctx, "chstorage.traces.SelectSpansets",
		trace.WithAttributes(
			attribute.Stringer("traceql.span_matcher_operation", params.Op),
			xattribute.StringerSlice("traceql.matchers", params.Matchers),
			xattribute.UnixNano("traceql.range.start", params.Start),
			xattribute.UnixNano("traceql.range.end", params.End),
			xattribute.Duration("traceql.min_duration", params.MinDuration),
			xattribute.Duration("traceql.max_duration", params.MaxDuration),
			attribute.Int("traceql.limit", params.Limit),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		c     = newSpanColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.In(
					chsql.Ident("trace_id"),
					chsql.SubQuery(q.buildSpansetsQuery(table, span, params)),
				),
			).
			Order(chsql.Ident("start"), chsql.Asc)

		traces = map[otelstorage.TraceID][]tracestorage.Span{}
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.traceID.Rows(); i++ {
				span, err := c.Row(i)
				if err != nil {
					return err
				}
				traces[span.TraceID] = append(traces[span.TraceID], span)
			}
			return nil
		},

		Type:   "SelectSpansets",
		Signal: "traces",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	var (
		result     = make([]traceqlengine.Trace, 0, len(traces))
		spansCount int
	)
	for id, spans := range traces {
		spansCount += len(spans)
		result = append(result, traceqlengine.Trace{
			TraceID: id,
			Spans:   spans,
		})
	}
	span.AddEvent("spans_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_spans", spansCount),
		attribute.Int("chstorage.total_traces", len(result)),
	))

	return iterators.Slice(result), nil
}

func (q *Querier) buildSpansetsQuery(table string, span trace.Span, params traceqlengine.SelectSpansetsParams) *chsql.SelectQuery {
	var (
		dropped    int
		matchExprs = make([]chsql.Expr, 0, len(params.Matchers))
	)
	for _, matcher := range params.Matchers {
		expr, ok := getTraceQLMatcher(matcher)
		if !ok {
			dropped++
			continue
		}
		matchExprs = append(matchExprs, expr)
	}
	span.SetAttributes(
		attribute.Int("chstorage.unsupported_span_matchers", dropped),
		attribute.String("chstorage.table", table),
	)

	query := chsql.Select(table,
		chsql.Column("trace_id", nil)).
		Distinct(true).
		Where(traceInTimeRange(params.Start, params.End))

	if len(matchExprs) > 0 {
		if params.Op == traceql.SpansetOpAnd {
			query.Where(matchExprs...)
		} else {
			query.Where(chsql.JoinOr(matchExprs...))
		}
	}
	return query
}

func spanIsRoot() chsql.Expr {
	return chsql.Eq(
		chsql.Ident("parent_span_id"),
		chsql.Unhex(chsql.String(zeroSpanIDHex)),
	)
}

func traceInTimeRange(start, end time.Time) chsql.Expr {
	exprs := make([]chsql.Expr, 0, 2)
	if !start.IsZero() {
		exprs = append(exprs, chsql.Gte(
			chsql.ToUnixTimestamp64Nano(chsql.Ident("start")),
			chsql.UnixNano(start),
		))
	}
	if !end.IsZero() {
		exprs = append(exprs, chsql.Lte(
			chsql.ToUnixTimestamp64Nano(chsql.Ident("end")),
			chsql.UnixNano(end),
		))
	}
	return chsql.JoinAnd(exprs...)
}

func getTraceQLMatcher(matcher traceql.SpanMatcher) (e chsql.Expr, _ bool) {
	if matcher.Op == 0 {
		// Just query spans with this attribute.
		var (
			attr  = matcher.Attribute
			exprs = make([]chsql.Expr, 0, 3)
		)
		for column := range getTraceQLAttributeColumns(attr) {
			exprs = append(exprs, chsql.SimpleJSONHas(
				chsql.Ident(column),
				attr.Name,
			))
		}
		return chsql.JoinOr(exprs...), true
	}

	var op func(l, r chsql.Expr) chsql.Expr
	switch matcher.Op {
	case traceql.OpEq:
		op = chsql.Eq
	case traceql.OpNotEq:
		op = chsql.NotEq
	case traceql.OpGt:
		op = chsql.Gt
	case traceql.OpGte:
		op = chsql.Gte
	case traceql.OpLt:
		op = chsql.Lt
	case traceql.OpLte:
		op = chsql.Lte
	case traceql.OpRe:
		op = chsql.Match
	default:
		return e, false
	}

	value, ok := getTraceQLLiteral(matcher.Static)
	if !ok {
		// Unsupported yet.
		return e, false
	}

	switch attr := matcher.Attribute; attr.Prop {
	case traceql.SpanDuration:
		return op(
			chsql.Ident("duration_ns"),
			value,
		), true
	case traceql.SpanName:
		return op(
			chsql.Ident("name"),
			value,
		), true
	case traceql.SpanStatus:
		return op(
			chsql.Ident("status_code"),
			value,
		), true
	case traceql.SpanKind:
		return op(
			chsql.Ident("kind"),
			value,
		), true
	case traceql.SpanStatusMessage:
		return op(
			chsql.Ident("status_message"),
			value,
		), true
	case traceql.SpanID:
		return op(
			chsql.Ident("span_id"),
			chsql.Unhex(value),
		), true
	case traceql.ParentID:
		return op(
			chsql.Ident("parent_span_id"),
			chsql.Unhex(value),
		), true
	case traceql.TraceID:
		return op(
			chsql.Ident("trace_id"),
			chsql.Unhex(value),
		), true
	case traceql.InstrumentationName:
		return op(
			chsql.Ident("scope_name"),
			value,
		), true
	case traceql.InstrumentationVersion:
		return op(
			chsql.Ident("scope_version"),
			value,
		), true
	case traceql.EventName:
		return arrayContainsOp(matcher.Op, chsql.Ident("events_names"), value)
	case traceql.EventTimeSinceStart:
		// TODO(tdakkota): requires arrayExists with lambda support.
		return e, false
	case traceql.LinkTraceID:
		return arrayContainsOp(matcher.Op, chsql.Ident("links_trace_ids"), chsql.Unhex(value))
	case traceql.LinkSpanID:
		return arrayContainsOp(matcher.Op, chsql.Ident("links_span_ids"), chsql.Unhex(value))
	case traceql.SpanParent,
		traceql.SpanChildCount,
		traceql.RootSpanName,
		traceql.RootServiceName,
		traceql.TraceDuration,
		traceql.NestedSetLeft,
		traceql.NestedSetRight,
		traceql.NestedSetParent:
		// Unsupported yet.
		return e, false
	default:
		// SpanAttribute
		switch attribute.Key(attr.Name) {
		case semconv.ServiceNamespaceKey:
			return op(
				chsql.Ident("service_namespace"),
				value,
			), true
		case semconv.ServiceNameKey:
			return op(
				chsql.Ident("service_name"),
				value,
			), true
		case semconv.ServiceInstanceIDKey:
			return op(
				chsql.Ident("service_instance_id"),
				value,
			), true
		default:
			exprs := make([]chsql.Expr, 0, 3)
			for column := range getTraceQLAttributeColumns(attr) {
				exprs = append(exprs, op(
					attrSelector(column, attr.Name),
					chsql.ToString(value),
				))
			}
			return chsql.JoinOr(exprs...), true
		}
	}
}

func getTraceQLLiteral(s traceql.Static) (value chsql.Expr, _ bool) {
	switch s.Type {
	case traceql.TypeString:
		return chsql.String(s.AsString()), true
	case traceql.TypeInt:
		return chsql.Integer(s.AsInt()), true
	case traceql.TypeNumber:
		return chsql.Float(s.AsNumber()), true
	case traceql.TypeBool:
		return chsql.Bool(s.AsBool()), true
	case traceql.TypeDuration:
		return chsql.Integer(s.AsDuration().Nanoseconds()), true
	case traceql.TypeSpanStatus:
		return chsql.Integer(int(s.AsSpanStatus())), true
	case traceql.TypeSpanKind:
		return chsql.Integer(int(s.AsSpanKind())), true
	default:
		return value, false
	}
}

// arrayContainsOp returns a ClickHouse expression checking whether an array column
// contains (or does not contain) elem, for the given TraceQL binary op.
// Only OpEq and OpNotEq are supported; other ops return (zero, false).
func arrayContainsOp(op traceql.BinaryOp, arr, elem chsql.Expr) (chsql.Expr, bool) {
	has := chsql.Has(arr, elem)
	switch op {
	case traceql.OpEq:
		return has, true
	case traceql.OpNotEq:
		return chsql.Not(has), true
	default:
		return chsql.Expr{}, false
	}
}

func getTraceQLAttributeColumns(attr traceql.Attribute) iter.Seq[string] {
	if attr.Prop != traceql.SpanAttribute || attr.Parent {
		return emptySeq[string]
	}
	switch attr.Scope {
	case traceql.ScopeNone:
		return func(yield func(string) bool) {
			_ = yield(colAttrs) &&
				yield(colResource) &&
				yield(colScope)
		}
	case traceql.ScopeResource:
		return func(yield func(string) bool) {
			_ = yield(colScope) &&
				yield(colResource)
		}
	case traceql.ScopeSpan:
		return func(yield func(string) bool) {
			yield(colAttrs)
		}
	case traceql.ScopeInstrumentation:
		return func(yield func(string) bool) {
			yield(colScope)
		}
	default:
		// ScopeEvent and ScopeLink store attributes in array columns;
		// attribute-level pushdown is not supported for those scopes.
		return emptySeq[string]
	}
}

func emptySeq[V any](func(V) bool) {}
