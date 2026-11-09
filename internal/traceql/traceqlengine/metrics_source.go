package traceqlengine

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/scarecrow/kernel"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// MetricsQuerier supplies projected spans or complete candidate traces without
// applying search's whole-trace containment rule to the metrics time window.
type MetricsQuerier interface {
	SelectMetricsSpans(context.Context, SelectSpansetsParams) (iterators.Iterator[tracestorage.Span], error)
	SelectMetricsSpansets(context.Context, SelectSpansetsParams) (iterators.Iterator[Trace], error)
	WithQueryBudget(context.Context) context.Context
}

type metricsGroupKey [5]traceql.Static

type spanMetricsSource struct {
	querier                     MetricsQuerier
	grid                        metricsGrid
	limits                      metricsLimits
	scanned, groups, labelBytes int
}

func spanLocalMetrics(expr traceql.Expr, fields []traceql.Attribute) bool {
	pipeline, ok := expr.(*traceql.SpansetPipeline)
	if !ok || len(pipeline.Pipeline) != 1 {
		return false
	}
	if _, ok := pipeline.Pipeline[0].(*traceql.SpansetFilter); !ok {
		return false
	}
	for _, field := range fields {
		if field.Parent {
			return false
		}
		switch field.Prop {
		case traceql.SpanChildCount, traceql.SpanParent, traceql.RootSpanName, traceql.RootServiceName, traceql.TraceDuration,
			traceql.NestedSetLeft, traceql.NestedSetRight, traceql.NestedSetParent:
			return false
		}
	}
	return true
}

func (s *spanMetricsSource) Aggregation(ctx context.Context, agg *traceql.MetricsAggregation, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	if agg.Op != traceql.MetricsOpCountOverTime && agg.Op != traceql.MetricsOpRate {
		return nil, &UnsupportedError{Msg: "only count_over_time and rate metrics aggregations are supported"}
	}
	if len(agg.By) > 5 {
		return nil, &MetricsParamsError{Msg: "metrics support at most five grouping attributes"}
	}
	fields := referencedFields(agg.Spanset)
	groups := make([]Evaluater, len(agg.By))
	names := make([]string, len(agg.By))
	for i, attr := range agg.By {
		if attr.Scope == traceql.ScopeEvent || attr.Scope == traceql.ScopeLink || attr.Prop == traceql.EventName || attr.Prop == traceql.EventTimeSinceStart || attr.Prop == traceql.LinkSpanID || attr.Prop == traceql.LinkTraceID {
			return nil, &UnsupportedError{Msg: "event/link grouping in metrics is not supported yet"}
		}
		name := attr.String()
		if slices.Contains(names[:i], name) {
			return nil, &MetricsParamsError{Msg: "duplicate metrics grouping attribute"}
		}
		var err error
		groups[i], err = buildEvaluater(&agg.By[i])
		if err != nil {
			return nil, err
		}
		names[i] = name
		if !slices.Contains(fields, attr) {
			fields = append(fields, attr)
		}
	}
	op, matchers := traceql.ExtractMatchers(agg.Spanset)
	start := s.grid.start
	if !s.grid.instant {
		start++
	}
	params := SelectSpansetsParams{
		Op: op, Matchers: matchers, Fields: fields,
		Start: time.Unix(0, start), End: time.Unix(0, s.grid.end),
	}
	counts := map[metricsGroupKey]*scarecrow.Column{}
	series := []labels.Labels{}
	columns := []*scarecrow.Column{}
	builder := labels.NewBuilder(labels.EmptyLabels())
	newGroup := func(key metricsGroupKey) (*scarecrow.Column, error) {
		if s.groups >= s.limits.series {
			return nil, &MetricsLimitError{Msg: "metrics series limit exceeded"}
		}
		if err := ec.ChargeSamples(len(s.grid.steps)); err != nil {
			return nil, &MetricsLimitError{Msg: err.Error()}
		}
		builder.Reset(labels.EmptyLabels())
		hasLabels := len(names) == 0
		if len(names) == 0 {
			builder.Set("__name__", agg.Op.String())
		}
		for i, name := range names {
			value := key[i]
			if value.Type == traceql.TypeNil {
				continue
			}
			if value.Type == traceql.TypeNumber && (math.IsNaN(value.AsNumber()) || math.IsInf(value.AsNumber(), 0)) {
				return nil, &UnsupportedError{Msg: "non-finite metrics grouping values are unsupported"}
			}
			encoded := encodeMetricsLabel(value)
			s.labelBytes += len(name) + len(encoded)
			if s.labelBytes > 8<<20 {
				return nil, &MetricsLimitError{Msg: "metrics grouping labels exceed 8 MiB"}
			}
			builder.Set(name, encoded)
			hasLabels = true
		}
		if !hasLabels {
			builder.Set(names[0], encodeMetricsLabel(traceql.Static{Type: traceql.TypeNil}))
		}
		column := scarecrow.NewColumn(scarecrow.SeriesRef(len(columns)), len(s.grid.steps))
		for i := range column.V {
			column.Set(i, 0)
		}
		counts[key] = column
		series = append(series, builder.Labels())
		columns = append(columns, column)
		s.groups++
		return column, nil
	}
	if len(names) == 0 {
		if _, err := newGroup(metricsGroupKey{}); err != nil {
			return nil, err
		}
	}
	observe := func(span tracestorage.Span, ectx EvaluateCtx) error {
		bucket, ok := s.grid.bucket(int64(span.Start))
		if !ok {
			return nil
		}
		var key metricsGroupKey
		for i, group := range groups {
			key[i] = group.Eval(span, ectx)
		}
		column := counts[key]
		if column == nil {
			var err error
			column, err = newGroup(key)
			if err != nil {
				return err
			}
		}
		column.V[bucket]++
		return nil
	}
	if spanLocalMetrics(agg.Spanset, fields) {
		filter, err := buildEvaluater(agg.Spanset.(*traceql.SpansetPipeline).Pipeline[0].(*traceql.SpansetFilter).Expr)
		if err != nil {
			return nil, err
		}
		input, err := s.querier.SelectMetricsSpans(ctx, params)
		if err != nil {
			return nil, err
		}
		defer func() { _ = input.Close() }()
		var span tracestorage.Span
		for input.Next(&span) {
			if err := s.chargeSpan(ctx); err != nil {
				return nil, err
			}
			if value := filter.Eval(span, EvaluateCtx{}); value.Type == traceql.TypeBool && value.AsBool() {
				if err := observe(span, EvaluateCtx{}); err != nil {
					return nil, err
				}
			}
		}
		if err := input.Err(); err != nil {
			return nil, err
		}
	} else {
		processor, err := BuildExpr(agg.Spanset)
		if err != nil {
			return nil, err
		}
		input, err := s.querier.SelectMetricsSpansets(ctx, params)
		if err != nil {
			return nil, err
		}
		defer func() { _ = input.Close() }()
		var trace Trace
		for input.Next(&trace) {
			for range trace.Spans {
				if err := s.chargeSpan(ctx); err != nil {
					return nil, err
				}
			}
			set := metricsTraceSet(trace)
			sets, err := processor.Process([]Spanset{set})
			if err != nil {
				return nil, err
			}
			seen := map[[8]byte]struct{}{}
			for _, selected := range sets {
				for _, span := range selected.Spans {
					id := [8]byte(span.SpanID)
					if _, duplicate := seen[id]; duplicate {
						continue
					}
					seen[id] = struct{}{}
					if err := observe(span, set.evaluateCtx()); err != nil {
						return nil, err
					}
				}
			}
		}
		if err := input.Err(); err != nil {
			return nil, err
		}
	}
	if agg.Op == traceql.MetricsOpRate {
		seconds := float64(s.grid.step) / float64(time.Second)
		for _, column := range columns {
			kernel.ScaleF64(column.V, 1/seconds)
		}
	}
	return &metricsColumns{schema: scarecrow.NewSchema(series), columns: columns}, nil
}

func (s *spanMetricsSource) chargeSpan(ctx context.Context) error {
	s.scanned++
	if s.scanned > s.limits.spans {
		return &MetricsLimitError{Msg: "metrics span scan limit exceeded"}
	}
	if s.scanned&127 == 0 {
		return ctx.Err()
	}
	return nil
}

func metricsTraceSet(trace Trace) Spanset {
	set := Spanset{TraceID: trace.TraceID, Spans: trace.Spans}
	if len(trace.Spans) == 0 {
		return set
	}
	set.Start = trace.Spans[0].Start.AsTime()
	end := trace.Spans[0].End.AsTime()
	rootFound := false
	for _, span := range trace.Spans {
		if span.Start.AsTime().Before(set.Start) {
			set.Start = span.Start.AsTime()
		}
		if span.End.AsTime().After(end) {
			end = span.End.AsTime()
		}
		if !rootFound && span.ParentSpanID.IsEmpty() {
			rootFound = true
			set.RootSpanName = span.Name
			set.RootServiceName, _ = span.ServiceName()
		}
	}
	set.TraceDuration = end.Sub(set.Start)
	return set
}

type metricsColumns struct {
	schema  *scarecrow.Schema
	columns []*scarecrow.Column
	next    int
}

func (*metricsColumns) String() string                                      { return "TraceMetricsBuckets" }
func (*metricsColumns) Children() []scarecrow.Operator                      { return nil }
func (s *metricsColumns) Close() error                                      { s.columns = nil; return nil }
func (s *metricsColumns) Schema(context.Context) (*scarecrow.Schema, error) { return s.schema, nil }
func (s *metricsColumns) Next(ctx context.Context) (*scarecrow.Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.next >= len(s.columns) {
		return nil, nil
	}
	column := s.columns[s.next]
	s.next++
	return column, nil
}
