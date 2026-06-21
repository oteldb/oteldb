// Package chtracker provides Clickhouse query tracker.
package chtracker

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/tempoapi"
)

// Tracker is a query tracker.
type Tracker[Q any] struct {
	senderName string
	trace      bool

	batchSpanProcessor sdktrace.SpanProcessor
	tracerProvider     trace.TracerProvider
	tracer             trace.Tracer

	tempo *tempoapi.Client

	queriesMux sync.Mutex
	queries    []TrackedQuery[Q]
}

// TrackedQuery stores info about tracked query.
type TrackedQuery[Q any] struct {
	TraceID  string
	Duration time.Duration
	Timeout  bool
	Meta     Q
}

// Track creates a tracked span and calls given callback.
func (t *Tracker[Q]) Track(ctx context.Context, meta Q, cb func(context.Context, Q) error) (rerr error) {
	start := time.Now()

	var traceID string
	if t.trace {
		traceCtx, span := t.tracer.Start(ctx, "chtracker.Track",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		traceID = span.SpanContext().TraceID().String()
		ctx = traceCtx

		defer func() {
			if rerr != nil {
				span.RecordError(rerr)
				span.SetStatus(codes.Error, rerr.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}()
	}

	var (
		timeout bool
		sendErr = cb(ctx, meta)
	)
	switch {
	case sendErr == nil:
	case errors.Is(sendErr, context.DeadlineExceeded):
		timeout = true
	default:
		return errors.Wrap(sendErr, "send tracked")
	}
	duration := time.Since(start)

	t.queriesMux.Lock()
	t.queries = append(t.queries, TrackedQuery[Q]{
		TraceID:  traceID,
		Duration: duration,
		Timeout:  timeout,
		Meta:     meta,
	})
	t.queriesMux.Unlock()
	return nil
}

// Report iterates over tracked queries.
func (t *Tracker[Q]) Report(ctx context.Context, cb func(context.Context, TrackedQuery[Q], []QueryReport, error) error) error {
	if err := t.Flush(ctx); err != nil {
		return err
	}

	t.queriesMux.Lock()
	defer t.queriesMux.Unlock()

	grp, grpCtx := errgroup.WithContext(ctx)
	type retrivalResult struct {
		Reports []QueryReport
		Err     error
	}
	queries := make([]retrivalResult, len(t.queries))
	for i, tq := range t.queries {
		grp.Go(func() error {
			r, err := t.retrieveReports(grpCtx, tq)
			if err != nil {
				err = errors.Wrapf(err, "retrieve reports for %q", tq.TraceID)
			}
			queries[i] = retrivalResult{Reports: r, Err: err}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "retrieve reports")
	}

	for i, result := range queries {
		tq := t.queries[i]

		if err := cb(ctx, tq, result.Reports, result.Err); err != nil {
			return errors.Wrap(err, "report callback")
		}
	}

	return nil
}

// HTTPClient returns instrumented HTTP client to use.
func (t *Tracker[Q]) HTTPClient() *http.Client {
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(t.TracerProvider()),
			otelhttp.WithPropagators(propagator),
		),
	}
}

// TracerProvider returns tracer provider to use.
func (t *Tracker[Q]) TracerProvider() trace.TracerProvider {
	return t.tracerProvider
}

// Flush writes buffered traces to storage.
func (t *Tracker[Q]) Flush(ctx context.Context) error {
	if !t.trace {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	if err := t.batchSpanProcessor.ForceFlush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}
	return nil
}

// SetupOptions defines options for [Setup].
type SetupOptions struct {
	// Trace enables tracing.
	Trace bool
	// TempoAddr sets URL to Tempo API to retrieve traces.
	TempoAddr string
	// OTLPEndpoint sets gRPC endpoint to export traces to.
	OTLPEndpoint string
}

func (opts *SetupOptions) setDefaults() {
	if opts.TempoAddr == "" {
		opts.TempoAddr = "http://127.0.0.1:3200"
	}
	if opts.OTLPEndpoint == "" {
		// Avoid relying on gRPC's default "localhost:4317" target: its DNS
		// resolver can produce zero addresses on hosts with a broken or
		// minimal resolver setup (e.g. fresh VDS instances).
		opts.OTLPEndpoint = "127.0.0.1:4317"
	}
}

// Setup creates new [Tracker].
func Setup[Q any](ctx context.Context, senderName string, opts SetupOptions) (*Tracker[Q], error) {
	opts.setDefaults()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(opts.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create exporter")
	}
	t := &Tracker[Q]{
		senderName: senderName,
		trace:      opts.Trace,
	}

	t.batchSpanProcessor = sdktrace.NewBatchSpanProcessor(exporter)
	t.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", "otelbench."+senderName),
		)),
		sdktrace.WithSpanProcessor(t.batchSpanProcessor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.tracer = t.tracerProvider.Tracer(senderName)

	tempoClient, err := tempoapi.NewClient(opts.TempoAddr)
	if err != nil {
		return nil, errors.Wrap(err, "create tempo client")
	}
	t.tempo = tempoClient
	return t, nil
}
