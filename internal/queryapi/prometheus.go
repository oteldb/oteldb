package queryapi

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/prometheus/prometheus/storage"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/scarecrow"
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

// NewPromEngine builds the Scarecrow PromQL engine.
func NewPromEngine(opts PrometheusOptions) (promhandler.Engine, error) {
	return NewScarecrowEngine(opts.Logger, opts.Querier, opts.Config, opts.TracerProvider), nil
}

// NewScarecrowEngine uses the querier's backend Scanner when available, otherwise adapting
// storage.Queryable to the Scarecrow sample stream.
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
	if backend, ok := q.(interface{ ScarecrowScanner() scarecrow.Scanner }); ok {
		opts.NewScanner = func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }
		lg.Info("Using scarecrow PromQL engine with the backend Scanner")
	} else {
		lg.Info("Using scarecrow PromQL engine over the storage.Queryable adapter")
	}

	return scarecrow.NewEngine(opts)
}
