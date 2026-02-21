// Package httpmiddleware contains HTTP middlewares.
package httpmiddleware

import (
	"net/http"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Middleware is a net/http middleware.
type Middleware = func(http.Handler) http.Handler

// InjectLogger injects logger into request context.
func InjectLogger(lg *zap.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCtx := r.Context()
			reqCtx = zctx.WithOpenTelemetryZap(reqCtx)
			req := r.WithContext(zctx.Base(reqCtx, lg))
			next.ServeHTTP(w, req)
		})
	}
}

// LogRequests logs incoming requests using context logger.
func LogRequests(find RouteFinder) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			lg := zctx.From(ctx)
			var (
				opName = zap.Skip()
				opID   = zap.Skip()
			)
			if route, ok := find(r.Method, r.URL); ok {
				opName = zap.String("operationName", route.Name())
				opID = zap.String("operationId", route.OperationID())
			}
			lg.Info("Got request",
				zap.String("method", r.Method),
				zap.Stringer("url", r.URL),
				opID,
				opName,
			)
			next.ServeHTTP(w, r)
		})
	}
}

// Metrics wraps TracerProvider and MeterProvider.
type Metrics interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
	TextMapPropagator() propagation.TextMapPropagator
}

// Instrument setups otelhttp.
func Instrument(endpoint, serviceName string, find RouteFinder, m Metrics) Middleware {
	return func(h http.Handler) http.Handler {
		return otelhttp.NewHandler(h, "",
			otelhttp.WithPropagators(m.TextMapPropagator()),
			otelhttp.WithTracerProvider(m.TracerProvider()),
			otelhttp.WithMeterProvider(m.MeterProvider()),
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
			otelhttp.WithServerName(endpoint),
			otelhttp.WithMetricAttributesFn(func(r *http.Request) []attribute.KeyValue {
				route, ok := find(r.Method, r.URL)
				if !ok {
					return []attribute.KeyValue{
						attribute.String("oas.operation.id", "<unknown>"),
						attribute.String("oteldb.api", serviceName),
					}
				}
				return []attribute.KeyValue{
					attribute.String("oas.route.name", route.Name()),
					attribute.String("oas.operation.id", route.OperationID()),
					attribute.String("oteldb.api", serviceName),
				}
			}),
			otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
				op, ok := find(r.Method, r.URL)
				if ok {
					return serviceName + "." + op.OperationID()
				}
				return operation
			}),
		)
	}
}

// Wrap handler using given middlewares.
func Wrap(h http.Handler, middlewares ...Middleware) http.Handler {
	switch len(middlewares) {
	case 0:
		return h
	case 1:
		return middlewares[0](h)
	default:
		for i := len(middlewares) - 1; i >= 0; i-- {
			h = middlewares[i](h)
		}
		return h
	}
}
