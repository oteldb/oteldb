package queryapi

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/tempohandler"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// TempoOptions configures the Tempo API server.
type TempoOptions struct {
	// Querier serves the API.
	Querier TraceQuerier
	// TracerProvider and MeterProvider instrument the server and the engine.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// NewTempo builds the Tempo API server.
func NewTempo(opts TempoOptions) (*tempoapi.Server, error) {
	engine := traceqlengine.NewEngine(opts.Querier, traceqlengine.Options{
		TracerProvider: opts.TracerProvider,
	})
	handler := tempohandler.NewTempoAPI(opts.Querier, engine, tempohandler.TempoAPIOptions{})

	return tempoapi.NewServer(handler,
		tempoapi.WithAttributes(attribute.String("oteldb.api", "tempo")),
		tempoapi.WithTracerProvider(opts.TracerProvider),
		tempoapi.WithMeterProvider(opts.MeterProvider),
	)
}
