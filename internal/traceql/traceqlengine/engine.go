// Package traceqlengine implements TraceQL evaluation engine.
package traceqlengine

import (
	"context"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
	"github.com/oteldb/oteldb/internal/xattribute"
)

// Engine is a TraceQL evaluation engine.
type Engine struct {
	querier Querier

	tracer trace.Tracer
}

// Options sets Engine options.
type Options struct {
	// TracerProvider provides OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates new Engine.
func NewEngine(querier Querier, opts Options) *Engine {
	opts.setDefaults()

	return &Engine{
		querier: querier,
		tracer:  opts.TracerProvider.Tracer("traceql.Engine"),
	}
}

// EvalParams sets evaluation parameters.
type EvalParams struct {
	// Trace duration to search, optional.
	MinDuration time.Duration
	MaxDuration time.Duration
	// Time range to search, optional.
	Start time.Time
	End   time.Time
	Limit int
}

// Eval parses and evaluates query.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (traces *tempoapi.Traces, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("traceql.query", query),
			xattribute.Duration("traceql.params.min_duration", params.MinDuration),
			xattribute.Duration("traceql.params.max_duration", params.MaxDuration),
			xattribute.UnixNano("traceql.params.start", params.Start),
			xattribute.UnixNano("traceql.params.end", params.End),
			attribute.Int("traceql.params.limit", params.Limit),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else if traces != nil {
			var spans int
			for _, m := range traces.Traces {
				spans += len(m.SpanSet.Value.Spans)
			}
			span.AddEvent("return_result", trace.WithAttributes(
				attribute.Int("traceql.total_spansets", len(traces.Traces)),
				attribute.Int("traceql.total_spans", spans),
			))
		}
		span.End()
	}()

	expr, err := traceql.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	return e.evalExpr(ctx, expr, params)
}

func (e *Engine) evalExpr(ctx context.Context, expr traceql.Expr, params EvalParams) (*tempoapi.Traces, error) {
	processor, err := BuildExpr(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build pipeline")
	}
	// Only propagate attributes actually referenced by the query, matching
	// Tempo's search API behavior. See [referencedAttributes].
	allowedAttrs := referencedAttributes(expr)

	iter, err := e.querier.SelectSpansets(
		ctx,
		extractPredicates(expr, params),
	)
	if err != nil {
		return nil, errors.Wrap(err, "select spansets")
	}
	defer func() {
		_ = iter.Close()
	}()

	var (
		tr = timeRange{
			start: params.Start,
			end:   params.End,
			min:   params.MinDuration,
			max:   params.MaxDuration,
		}

		limit  = params.Limit
		elem   Trace
		result []tempoapi.TraceSearchMetadata
	)
	if limit < 0 {
		limit = 20
	}
	for iter.Next(&elem) {
		if len(result) >= limit {
			break
		}
		if len(elem.Spans) < 1 {
			continue
		}

		var (
			root  = elem.Spans[0]
			start = root.Start.AsTime()
			end   = root.End.AsTime()
		)
		for _, span := range elem.Spans[1:] {
			if st := span.Start.AsTime(); st.Before(start) {
				start = st
			}
			if et := span.End.AsTime(); et.After(end) {
				end = et
			}
			if !root.ParentSpanID.IsEmpty() && span.ParentSpanID.IsEmpty() {
				root = span
			}
		}

		var rootServiceName string
		if name, ok := root.ServiceName(); ok {
			rootServiceName = name
		}

		if !tr.within(start, end) {
			// Trace is not within range.
			continue
		}

		ss := []Spanset{
			{
				TraceID:         elem.TraceID,
				Spans:           elem.Spans,
				RootSpanName:    root.Name,
				RootServiceName: rootServiceName,
				Start:           start,
				TraceDuration:   end.Sub(start),
			},
		}

		var err error
		ss, err = processor.Process(ss)
		if err != nil {
			return nil, errors.Wrapf(err, "process trace %s", elem.TraceID.Hex())
		}

		for _, s := range ss {
			if len(result) >= limit {
				break
			}

			var spans tempoapi.TempoSpanSet
			for _, span := range s.Spans {
				spans.Spans = append(spans.Spans, span.AsTempoSpanFiltered(allowedAttrs))
			}

			// Add attributes from root, referenced by the query only.
			tracestorage.ConvertToTempoAttrsFiltered(&spans.Attributes, root.ScopeAttrs, allowedAttrs)
			tracestorage.ConvertToTempoAttrsFiltered(&spans.Attributes, root.ResourceAttrs, allowedAttrs)
			result = append(result, tempoapi.TraceSearchMetadata{
				TraceID:           s.TraceID.Hex(),
				RootServiceName:   tempoapi.NewOptString(s.RootServiceName),
				RootTraceName:     tempoapi.NewOptString(s.RootSpanName),
				StartTimeUnixNano: s.Start,
				DurationMs:        tempoapi.NewOptInt(int(s.TraceDuration.Milliseconds())),
				SpanSet:           tempoapi.NewOptTempoSpanSet(spans),
				SpanSets:          []tempoapi.TempoSpanSet{spans},
			})
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	slices.SortFunc(result, func(a, b tempoapi.TraceSearchMetadata) int {
		return a.StartTimeUnixNano.Compare(b.StartTimeUnixNano)
	})
	return &tempoapi.Traces{
		Traces:  result,
		Metrics: tempoapi.NewOptSearchMetrics(tempoapi.SearchMetrics{}),
	}, nil
}

type timeRange struct {
	start, end time.Time
	min, max   time.Duration
}

func (r timeRange) within(start, end time.Time) bool {
	if !r.start.IsZero() && start.Before(r.start) {
		return false
	}

	if !r.end.IsZero() && end.After(r.end) {
		return false
	}

	duration := end.Sub(start)
	if (r.min != 0 && duration < r.min) || (r.max != 0 && duration > r.max) {
		return false
	}

	return true
}

func extractPredicates(expr traceql.Expr, params EvalParams) SelectSpansetsParams {
	op, matchers := traceql.ExtractMatchers(expr)
	return SelectSpansetsParams{
		Op:          op,
		Matchers:    matchers,
		Start:       params.Start,
		End:         params.End,
		MinDuration: params.MinDuration,
		MaxDuration: params.MaxDuration,
		Limit:       params.Limit,
	}
}
