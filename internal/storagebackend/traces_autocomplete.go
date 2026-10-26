package storagebackend

import (
	"context"
	"iter"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func (q *TraceQuerier) filteredTagValues(ctx context.Context, attr traceql.Attribute, opts tracestorage.TagValuesOptions) (iterators.Iterator[tracestorage.Tag], error) {
	expr, err := traceql.Parse(opts.AutocompleteQuery.String())
	if err != nil {
		return nil, errors.Wrap(err, "parse autocomplete query")
	}
	processor, err := traceqlengine.BuildExpr(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build autocomplete query")
	}
	fields := []traceql.Attribute{attr}
	for _, matcher := range opts.AutocompleteQuery.Matchers {
		fields = append(fields, matcher.Attribute)
	}
	input, err := q.autocompleteInput(ctx, attr, opts, fields)
	if err != nil {
		return nil, err
	}
	seen := map[string]tracestorage.Tag{}
	for trace, err := range input {
		if err != nil {
			return nil, err
		}
		sets, err := processor.Process([]traceqlengine.Spanset{autocompleteSpanset(trace)})
		if err != nil {
			return nil, errors.Wrap(err, "evaluate autocomplete query")
		}
		for _, set := range sets {
			collectAutocompleteTags(seen, attr, set.Spans)
		}
	}
	return iterators.Slice(sortedTags(seen)), nil
}

func (q *TraceQuerier) autocompleteInput(ctx context.Context, attr traceql.Attribute, opts tracestorage.TagValuesOptions, fields []traceql.Attribute) (iter.Seq2[traceqlengine.Trace, error], error) {
	if !identityAutocomplete(attr, opts.AutocompleteQuery.Matchers) {
		input, err := q.SelectSpansets(ctx, traceqlengine.SelectSpansetsParams{
			Op: traceql.SpansetOpAnd, Matchers: opts.AutocompleteQuery.Matchers,
			Fields: fields, Start: opts.Start, End: opts.End,
		})
		if err != nil {
			return nil, err
		}
		return func(yield func(traceqlengine.Trace, error) bool) {
			defer func() { _ = input.Close() }()
			var trace traceqlengine.Trace
			for input.Next(&trace) {
				if !yield(trace, nil) {
					return
				}
			}
			if err := input.Err(); err != nil {
				yield(traceqlengine.Trace{}, err)
			}
		}, nil
	}
	var matchers []fetch.Matcher
	for _, matcher := range opts.AutocompleteQuery.Matchers {
		groups, ok := lowerSpanMatcher(matcher)
		if ok && len(groups) == 1 && len(groups[0].conditions) == 0 {
			matchers = append(matchers, groups[0].matchers...)
		}
	}
	lo, hi := seriesWindow(opts.Start, opts.End)
	series, err := q.b.src.TraceSeries(ctx, q.b.tenant, matchers, lo, hi)
	if err != nil {
		return nil, errors.Wrap(err, "autocomplete trace series")
	}
	return func(yield func(traceqlengine.Trace, error) bool) {
		var spans [1]tracestorage.Span
		for _, s := range series {
			spans[0] = autocompleteIdentity(s)
			if !yield(traceqlengine.Trace{Spans: spans[:]}, nil) {
				return
			}
		}
	}, nil
}

func identityAutocomplete(attr traceql.Attribute, matchers []traceql.SpanMatcher) bool {
	streamAttribute := func(attr traceql.Attribute) bool {
		return !attr.Parent && attr.Prop == traceql.SpanAttribute &&
			(attr.Scope == traceql.ScopeResource || attr.Scope == traceql.ScopeInstrumentation)
	}
	if !streamAttribute(attr) && attr.Prop != traceql.InstrumentationName && attr.Prop != traceql.InstrumentationVersion {
		return false
	}
	for _, matcher := range matchers {
		if !streamAttribute(matcher.Attribute) {
			return false
		}
	}
	return true
}

func autocompleteIdentity(series signal.Series) tracestorage.Span {
	return tracestorage.Span{
		ResourceAttrs: otelAttrs(series.Resource.Attributes), ScopeAttrs: otelAttrs(series.Scope.Attributes),
		ScopeName: string(series.Scope.Name), ScopeVersion: string(series.Scope.Version),
	}
}

func autocompleteSpanset(trace traceqlengine.Trace) traceqlengine.Spanset {
	set := traceqlengine.Spanset{TraceID: trace.TraceID, Spans: trace.Spans}
	if len(trace.Spans) == 0 {
		return set
	}
	set.Start = trace.Spans[0].Start.AsTime()
	end := trace.Spans[0].End.AsTime()
	root := false
	for _, span := range trace.Spans {
		if start := span.Start.AsTime(); start.Before(set.Start) {
			set.Start = start
		}
		if stop := span.End.AsTime(); stop.After(end) {
			end = stop
		}
		if !root && span.ParentSpanID.IsEmpty() {
			root = true
			set.RootSpanName = span.Name
			set.RootServiceName, _ = span.ServiceName()
		}
	}
	set.TraceDuration = end.Sub(set.Start)
	return set
}

func collectAutocompleteTags(seen map[string]tracestorage.Tag, attr traceql.Attribute, spans []tracestorage.Span) {
	add := func(scope traceql.AttributeScope, name, value string) {
		seen[value] = tracestorage.Tag{Name: name, Value: value, Type: traceql.TypeString, Scope: scope}
	}
	for _, span := range spans {
		switch attr.Prop {
		case traceql.RootSpanName:
			if span.ParentSpanID.IsEmpty() && span.Name != "" {
				add(traceql.ScopeNone, attr.String(), span.Name)
			}
		case traceql.RootServiceName:
			if span.ParentSpanID.IsEmpty() {
				if service, ok := span.ServiceName(); ok && service != "" {
					add(traceql.ScopeNone, attr.String(), service)
				}
			}
		case traceql.SpanName:
			add(traceql.ScopeNone, attr.String(), span.Name)
		case traceql.SpanStatusMessage:
			add(traceql.ScopeNone, attr.String(), span.StatusMessage)
		case traceql.InstrumentationName:
			add(traceql.ScopeNone, attr.String(), span.ScopeName)
		case traceql.InstrumentationVersion:
			add(traceql.ScopeNone, attr.String(), span.ScopeVersion)
		case traceql.SpanAttribute:
			forEachSpanTag(span, func(scope traceql.AttributeScope, name, value string) {
				if name == attr.Name && (attr.Scope == traceql.ScopeNone || attr.Scope == scope) {
					add(scope, name, value)
				}
			})
		}
	}
}
