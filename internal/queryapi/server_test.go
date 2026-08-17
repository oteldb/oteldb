package queryapi_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/queryapi"
)

type testMetrics struct{}

func (testMetrics) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }
func (testMetrics) MeterProvider() metric.MeterProvider  { return metricnoop.NewMeterProvider() }

func (testMetrics) TextMapPropagator() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

type testRoute struct{}

func (testRoute) Name() string        { return "test" }
func (testRoute) OperationID() string { return "test" }

type testServer struct{}

func (testServer) FindPath(string, *url.URL) (testRoute, bool) { return testRoute{}, true }

func (testServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusTeapot)
}

// TestHTTPServer checks that the standard middleware stack is applied and that extra middlewares
// keep the order they are given in, since authentication relies on running before anything else.
func TestHTTPServer(t *testing.T) {
	t.Parallel()

	var order []string
	mark := func(name string) httpmiddleware.Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}

	srv := queryapi.HTTPServer(queryapi.ServerOptions{
		Name:    "test",
		Addr:    "127.0.0.1:0",
		Logger:  zap.NewNop(),
		Metrics: testMetrics{},
	}, testServer{}, mark("first"), mark("second"))

	require.Equal(t, "127.0.0.1:0", srv.Addr)
	require.Equal(t, 15*time.Second, srv.ReadHeaderTimeout)

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/query", http.NoBody))

	assert.Equal(t, http.StatusTeapot, rec.Code)
	assert.Equal(t, []string{"first", "second"}, order)
}

func TestHTTPServerReadHeaderTimeout(t *testing.T) {
	t.Parallel()

	srv := queryapi.HTTPServer(queryapi.ServerOptions{
		Name:              "test",
		Logger:            zap.NewNop(),
		Metrics:           testMetrics{},
		ReadHeaderTimeout: time.Second,
	}, testServer{})

	require.Equal(t, time.Second, srv.ReadHeaderTimeout)
}
