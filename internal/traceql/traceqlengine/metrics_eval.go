package traceqlengine

import (
	"context"
	"math"
	"slices"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/xspan"
)

// EvalMetrics executes count/rate metrics and shared result operators.
// Instant evaluation returns one sample per series over the selected interval.
func (e *Engine) EvalMetrics(ctx context.Context, query string, params MetricsEvalParams) (_ []tempoapi.TimeSeries, rerr error) {
	ctx, span := e.tracer.Start(ctx, "EvalMetrics", trace.WithAttributes(attribute.String("traceql.query", query)))
	defer func() { xspan.End(span, rerr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	grid, err := newMetricsGrid(params, e.metrics.steps)
	if err != nil {
		return nil, err
	}
	expr, err := traceql.Parse(query)
	if err != nil {
		return nil, &MetricsParamsError{Msg: err.Error()}
	}
	if _, ok := expr.(*traceql.HintedExpr); ok {
		return nil, &UnsupportedError{Msg: "metrics query hints are not supported yet"}
	}
	metric, ok := expr.(traceql.MetricsExpr)
	if !ok {
		return nil, &MetricsParamsError{Msg: "expected a TraceQL metrics query"}
	}
	if err := validateCountMetrics(metric); err != nil {
		return nil, err
	}
	querier, ok := e.querier.(MetricsQuerier)
	if !ok {
		return nil, &UnsupportedError{Msg: "backend does not support TraceQL metrics"}
	}
	ctx = querier.WithQueryBudget(ctx)
	source := &spanMetricsSource{querier: querier, grid: grid, limits: e.metrics}
	ec := scarecrow.NewEvalContext(grid.steps, e.metrics.samples)
	root, err := BuildMetrics(ctx, metric, source, ec)
	if err != nil {
		return nil, err
	}
	defer func() { rerr = errors.Join(rerr, root.Close()) }()
	schema, err := root.Schema(ctx)
	if err != nil {
		return nil, err
	}
	type outputSeries struct {
		key    string
		series tempoapi.TimeSeries
	}
	outputs := []outputSeries{}
	for {
		column, err := root.Next(ctx)
		if err != nil {
			return nil, err
		}
		if column == nil {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ls, err := metricsLabels(schema.At(column.Ref))
		if err != nil {
			return nil, err
		}
		series := tempoapi.TimeSeries{Labels: ls}
		for i, value := range column.V {
			if !column.IsSet(i) || math.IsNaN(value) {
				continue
			}
			if math.IsInf(value, 0) {
				return nil, &UnsupportedError{Msg: "non-finite metrics results are unsupported"}
			}
			if int64(int(grid.steps[i])) != grid.steps[i] {
				return nil, &UnsupportedError{Msg: "metrics timestamp exceeds the API integer range"}
			}
			series.Samples = append(series.Samples, tempoapi.Sample{TimestampMs: tempoapi.NewOptInt(int(grid.steps[i])), Value: tempoapi.NewOptFloat64(value)})
		}
		if len(series.Samples) > 0 {
			outputs = append(outputs, outputSeries{key: schema.At(column.Ref).String(), series: series})
		}
	}
	slices.SortFunc(outputs, func(a, b outputSeries) int { return strings.Compare(a.key, b.key) })
	result := make([]tempoapi.TimeSeries, len(outputs))
	for i := range outputs {
		result[i] = outputs[i].series
	}
	span.SetAttributes(attribute.Int("traceql.metrics.scanned_spans", source.scanned), attribute.Int("traceql.metrics.series", len(result)))
	return result, nil
}

func validateCountMetrics(expr traceql.MetricsExpr) error {
	switch expr := expr.(type) {
	case *traceql.MetricsAggregation:
		if expr.Op == traceql.MetricsOpCountOverTime || expr.Op == traceql.MetricsOpRate {
			return nil
		}
	case *traceql.MetricsPipeline:
		return validateCountMetrics(expr.Expr)
	case *traceql.MetricsBinaryExpr:
		if err := validateCountMetrics(expr.Left); err != nil {
			return err
		}
		return validateCountMetrics(expr.Right)
	}
	return &UnsupportedError{Msg: "only count_over_time and rate metrics aggregations are supported"}
}
