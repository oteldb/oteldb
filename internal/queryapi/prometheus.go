package queryapi

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/prometheus/prometheus/storage"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// PrometheusOptions configures the Prometheus API server.
type PrometheusOptions struct {
	// Config is the Prometheus API config. Defaults are applied to a copy of it.
	Config config.Prometheus
	// Querier serves the API.
	Querier MetricQuerier
	// Logger reports which PromQL engine is used.
	Logger *zap.Logger
	// TracerProvider and MeterProvider instrument the server and the engine.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// NewPrometheus builds the Prometheus API server.
func NewPrometheus(opts PrometheusOptions) (*promapi.Server, error) {
	engine, err := NewPromEngine(opts)
	if err != nil {
		return nil, err
	}

	handler := promhandler.NewPromAPI(engine, opts.Querier, opts.Querier, opts.Querier, promhandler.PromAPIOptions{})

	return promapi.NewServer(handler,
		promapi.WithAttributes(attribute.String("oteldb.api", "prom")),
		promapi.WithTracerProvider(opts.TracerProvider),
		promapi.WithMeterProvider(opts.MeterProvider),
		promapi.WithMiddleware(promhandler.TimeoutMiddleware()),
	)
}

// NewPromEngine builds the PromQL engine selected by the config.
func NewPromEngine(opts PrometheusOptions) (promhandler.Engine, error) {
	cfg := opts.Config
	cfg.SetDefaults()

	if cfg.EnableScarecrowEngine {
		return NewScarecrowEngine(opts.Logger, opts.Querier, cfg, opts.TracerProvider), nil
	}

	engine, err := promql.New(opts.Querier, promql.EngineOpts{
		// NOTE: zero-value MaxSamples and Timeout makes
		// all queries to fail with error.
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     cfg.EnableAtModifier,
		EnableNegativeOffset: *cfg.EnableNegativeOffset,
		EnablePerStepStats:   cfg.EnablePerStepStats,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create PromQL engine")
	}

	return engine, nil
}

// NewScarecrowEngine builds the internal/scarecrow engine. When q is the embedded storage engine,
// it gets scarecrow's native columnar Scanner (storagebackend.Backend.ScarecrowScanner); over any
// other querier (e.g. ClickHouse) it falls back to scarecrow's generic storage.Queryable adapter,
// which is correct but pays the same per-sample conversion cost the fork already does.
func NewScarecrowEngine(
	lg *zap.Logger,
	q MetricQuerier,
	cfg config.Prometheus,
	tracerProvider trace.TracerProvider,
) *scarecrow.Engine {
	cfg.SetDefaults()

	opts := scarecrow.Opts{
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     cfg.EnableAtModifier,
		EnableNegativeOffset: *cfg.EnableNegativeOffset,
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		TracerProvider:       tracerProvider,
	}

	if lg == nil {
		lg = zap.NewNop()
	}
	if backend, ok := q.(*storagebackend.Backend); ok {
		opts.NewScanner = func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }
		lg.Info("Using scarecrow PromQL engine with the native storage Scanner")
	} else {
		lg.Warn("Using scarecrow PromQL engine over the generic storage.Queryable adapter; " +
			"switch metrics.backend to storage for the native Scanner")
	}

	return scarecrow.NewEngine(opts)
}
