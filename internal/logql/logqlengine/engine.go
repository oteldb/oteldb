// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/logql"
)

// Engine is a LogQL evaluation engine.
type Engine struct {
	querier     Querier
	querierCaps QuerierCapabilities

	lookbackDuration time.Duration
	otelAdapter      bool
	parseOpts        logql.ParseOptions

	optimizers []Optimizer

	stats  engineStats
	meter  metric.Meter
	tracer trace.Tracer
}

// Options sets Engine options.
type Options struct {
	// LookbackDuration sets lookback duration for instant queries.
	//
	// Should be negative, otherwise default value would be used.
	LookbackDuration time.Duration

	// ParseOptions is a LogQL parser options.
	ParseOptions logql.ParseOptions

	// OTELAdapter enables 'otel adapter' whatever it is.
	OTELAdapter bool

	// Optimizers defines a list of optimiziers to use.
	Optimizers []Optimizer

	// MeterProvider provides OpenTelemetry meter for this engine.
	MeterProvider metric.MeterProvider

	// TracerProvider provides OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.LookbackDuration >= 0 {
		o.LookbackDuration = -30 * time.Second
	}
	if o.Optimizers == nil {
		o.Optimizers = DefaultOptimizers()
	}
	if o.MeterProvider == nil {
		o.MeterProvider = otel.GetMeterProvider()
	}
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates new Engine.
func NewEngine(querier Querier, opts Options) (*Engine, error) {
	opts.setDefaults()

	var (
		meter = opts.MeterProvider.Meter("logqlengine.Engine")
		stats engineStats
	)
	if err := stats.Register(meter); err != nil {
		return nil, errors.Wrap(err, "register metrics")
	}

	return &Engine{
		querier:          querier,
		querierCaps:      querier.Capabilities(),
		lookbackDuration: opts.LookbackDuration,
		otelAdapter:      opts.OTELAdapter,
		parseOpts:        opts.ParseOptions,
		optimizers:       opts.Optimizers,

		stats:  stats,
		meter:  meter,
		tracer: opts.TracerProvider.Tracer("logql.Engine"),
	}, nil
}

// ParseOptions returns [logql.ParseOptions] used by engine.
func (e *Engine) ParseOptions() logql.ParseOptions {
	return e.parseOpts
}

// NewQuery creates new [Query].
func (e *Engine) NewQuery(ctx context.Context, query string) (q Query, rerr error) {
	ctx, span := e.tracer.Start(ctx, "logql.Engine.NewQuery", trace.WithAttributes(
		attribute.String("logql.query", query),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	expr, err := logql.Parse(query, e.parseOpts)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}
	return e.newQuery(ctx, expr)
}

// NewQueryFromExpr creates new [Query] from parsed [logql.Expr].
func (e *Engine) NewQueryFromExpr(ctx context.Context, expr logql.Expr) (q Query, rerr error) {
	ctx, span := e.tracer.Start(ctx, "logql.Engine.NewQuery", trace.WithAttributes(
		// TODO(tdakkota): make LogQL printer.
		attribute.String("logql.query", "<query>"),
	))
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()
	return e.newQuery(ctx, expr)
}

func (e *Engine) newQuery(ctx context.Context, expr logql.Expr) (q Query, _ error) {
	if _, explain := expr.(*logql.ExplainExpr); explain {
		logs := new(explainLogs)
		ctx = buildExplainQuery(ctx, logs)

		defer func() {
			q = &ExplainQuery{
				Explain: q,
				logs:    logs,
			}
		}()
	}

	var err error
	q, err = e.buildQuery(ctx, expr)
	if err != nil {
		return nil, err
	}

	q, err = e.applyOptimizers(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "optimize")
	}

	return q, nil
}

func (e *Engine) buildQuery(ctx context.Context, expr logql.Expr) (_ Query, rerr error) {
	switch expr := logql.UnparenExpr(expr).(type) {
	case *logql.ExplainExpr:
		return e.buildQuery(ctx, expr.X)
	case *logql.LogExpr:
		return e.buildLogQuery(ctx, expr)
	case *logql.LiteralExpr:
		return e.buildLiteralQuery(ctx, expr)
	case logql.MetricExpr:
		return e.buildMetricQuery(ctx, expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
}
