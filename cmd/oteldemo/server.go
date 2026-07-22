package main

import (
	"context"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func newServerCommand() *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Run instrumented demo HTTP server",
		Args:  cobra.NoArgs,
		Run: func(*cobra.Command, []string) {
			app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
				return server(ctx, lg, m, addr)
			})
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "0.0.0.0:8080", "HTTP listen address")
	return cmd
}

func server(ctx context.Context, lg *zap.Logger, m *app.Telemetry, addr string) error {
	g, ctx := errgroup.WithContext(ctx)
	mux := http.NewServeMux()

	requestDurations := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "A histogram of the HTTP request durations in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 5),
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(requestDurations)

	// Expose /metrics HTTP endpoint using the created custom registry.
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	tracer := m.TracerProvider().Tracer("server")
	doProcessing := func(ctx context.Context, step string) {
		ctx, span := tracer.Start(ctx, "doProcessing",
			trace.WithAttributes(
				attribute.String("step", step),
			),
		)
		defer span.End()
		zctx.From(ctx).Info("processing", zap.String("step", step))
		// Sleep some random time.
		duration := time.Duration(100+rand.Intn(100)) * time.Millisecond // #nosec G404
		time.Sleep(duration)
		requestDurations.(prometheus.ExemplarObserver).ObserveWithExemplar(duration.Seconds(), prometheus.Labels{
			"trace_id": span.SpanContext().TraceID().String(),
			"span_id":  span.SpanContext().SpanID().String(),
		})
	}
	mux.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handleHello")
		defer span.End()
		zctx.From(ctx).Info("got request")
		time.Sleep(time.Millisecond * 40)
		doProcessing(ctx, "alpha")
		doProcessing(ctx, "beta")
		zctx.From(ctx).Info("processed")
		time.Sleep(time.Millisecond * 40)
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler: otelhttp.NewHandler(mux, "Hello",
			otelhttp.WithMeterProvider(m.MeterProvider()),
			otelhttp.WithTracerProvider(m.TracerProvider()),
			otelhttp.WithFilter(func(r *http.Request) bool {
				return r.URL.Path != "/metrics"
			}),
		),
	}
	g.Go(func() error {
		lg.Info("server listening", zap.String("addr", srv.Addr))
		defer lg.Info("server stopped")
		return srv.ListenAndServe()
	})
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(ctx)
	})
	return g.Wait()
}
