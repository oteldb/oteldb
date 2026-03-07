package integration

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type randomIDGenerator struct {
	sync.Mutex
	rand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *randomIDGenerator) NewSpanID(context.Context, trace.TraceID) (sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(sid[:]) // #nosec G104
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID from a
// randomly-chosen sequence.
func (gen *randomIDGenerator) NewIDs(context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(tid[:]) // #nosec G104
	gen.rand.Read(sid[:]) // #nosec G104
	return tid, sid
}

// Provider is a helper for tests providing a TracerProvider and an
// InMemoryExporter.
type Provider struct {
	*sdkmetric.MeterProvider
	Reader *sdkmetric.ManualReader
	*sdktrace.TracerProvider
	Exporter *tracetest.InMemoryExporter
}

// Reset clears the current in-memory storage.
func (p *Provider) Reset() {
	p.Exporter.Reset()
}

// Flush forces a flush of all finished spans.
func (p *Provider) Flush() {
	if err := p.MeterProvider.ForceFlush(context.Background()); err != nil {
		panic(err)
	}
	if err := p.TracerProvider.ForceFlush(context.Background()); err != nil {
		panic(err)
	}
}

// TraceProvider returns trace exporter for debugging.
//
// E2E_TRACES_EXPORTER enironment variable sets gRPC OTLP receiver address
// to export traces.
func TraceProvider(t *testing.T) trace.TracerProvider {
	if addr := os.Getenv("E2E_TRACES_EXPORTER"); addr != "" {
		ctx := context.Background()

		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(addr),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			t.Fatalf("create trace exporter: %+v", err)
		}
		tracer := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
		t.Cleanup(func() {
			flushCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := tracer.ForceFlush(flushCtx); err != nil {
				t.Logf("Flush traces error: %+v", err)
			}
			if err := tracer.Shutdown(ctx); err != nil {
				t.Logf("Shutdown tracer error: %+v", err)
			}
		})

		return tracer
	}
	return noop.NewTracerProvider()
}

// NewProvider initializes and returns a new Provider along with an exporter.
func NewProvider() *Provider {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	exporter := tracetest.NewInMemoryExporter()

	randSource := rand.NewSource(10)
	tp := sdktrace.NewTracerProvider(
		// Using deterministic random ids.
		sdktrace.WithIDGenerator(&randomIDGenerator{
			// #nosec G404
			rand: rand.New(randSource),
		}),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(0), // instant
		),
	)
	return &Provider{
		MeterProvider:  mp,
		Reader:         reader,
		TracerProvider: tp,
		Exporter:       exporter,
	}
}
