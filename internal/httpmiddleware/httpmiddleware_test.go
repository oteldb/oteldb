// Package httpmiddleware contains HTTP middlewares.
package httpmiddleware

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/go-faster/oteldb/integration"
)

type testHandler struct{}

func (*testHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

type testMiddleware struct{}

func (*testMiddleware) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestInjectLogger(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)

	h := Wrap(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lg := zctx.From(r.Context())
			lg.Info("Hello")
		}),
		InjectLogger(zap.New(core)),
	)
	h.ServeHTTP(nil, &http.Request{})

	entries := logs.FilterLevelExact(zapcore.InfoLevel).All()
	require.Len(t, entries, 1)

	entry := entries[0]
	require.Equal(t, "Hello", entry.Message)
}

type testOgenServer struct{}

func (*testOgenServer) FindPath(method string, u *url.URL) (r testOgenRoute, _ bool) {
	if method != http.MethodGet || u.Path != testOgenRoutePath {
		return r, false
	}
	return r, true
}

type testOgenRoute struct{}

const (
	testOgenRoutePath = "/foo"
	testOgenRouteName = "TestOgenRoute"
	testOgenRouteID   = "testOgenRoute"
)

func (testOgenRoute) Name() string        { return testOgenRouteName }
func (testOgenRoute) OperationID() string { return testOgenRouteID }

func TestLogRequests(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)

	h := Wrap(&testHandler{},
		InjectLogger(zap.New(core)),
		LogRequests(MakeRouteFinder(&testOgenServer{})),
	)
	h.ServeHTTP(nil, &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Path: "/unknown_ogen_path",
		},
	})
	h.ServeHTTP(nil, &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: testOgenRoutePath,
		},
	})

	entries := logs.FilterLevelExact(zapcore.InfoLevel).All()
	require.Len(t, entries, 2)

	entry := entries[0]
	require.Equal(t, "Got request", entry.Message)
	fields := entry.ContextMap()
	require.Len(t, fields, 2)
	require.Equal(t, http.MethodPost, fields["method"])
	require.Equal(t, "/unknown_ogen_path", fields["url"])

	entry = entries[1]
	require.Equal(t, "Got request", entry.Message)
	fields = entry.ContextMap()
	require.Len(t, fields, 4)
	require.Equal(t, http.MethodGet, fields["method"])
	require.Equal(t, testOgenRoutePath, fields["url"])
	require.Equal(t, "TestOgenRoute", fields["operationName"])
	require.Equal(t, "testOgenRoute", fields["operationId"])
}

func TestWrap(t *testing.T) {
	endpoint := &testHandler{}

	// Check case with zero middlewares.
	result := Wrap(endpoint)
	require.Equal(t, endpoint, result)

	// Check case with one middleware.
	middleware := &testMiddleware{}
	result = Wrap(endpoint, func(h http.Handler) http.Handler {
		return middleware
	})
	require.Equal(t, middleware, result)

	// Ensure order of wrapping.
	var (
		calls          []int
		callMiddleware = func(n int) Middleware {
			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls = append(calls, n)
					next.ServeHTTP(w, r)
				})
			}
		}
	)

	result = Wrap(endpoint,
		callMiddleware(1),
		callMiddleware(2),
		callMiddleware(3),
	)
	result.ServeHTTP(nil, nil)
	require.Equal(t, []int{1, 2, 3}, calls)
}

type testMetrics struct {
	meterProvider     metric.MeterProvider
	textMapPropagator propagation.TextMapPropagator
	tracerProvider    trace.TracerProvider
}

var _ Metrics = (*testMetrics)(nil)

// MeterProvider implements [Metrics].
func (t *testMetrics) MeterProvider() metric.MeterProvider {
	return t.meterProvider
}

// TextMapPropagator implements [Metrics].
func (t *testMetrics) TextMapPropagator() propagation.TextMapPropagator {
	return t.textMapPropagator
}

// TracerProvider implements [Metrics].
func (t *testMetrics) TracerProvider() trace.TracerProvider {
	return t.tracerProvider
}

func TestInstrument(t *testing.T) {
	const (
		code    = http.StatusOK
		addr    = "aboba"
		port    = 9090
		service = "abobapi"
	)

	provider := integration.NewProvider()
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
	})
	h := Wrap(fn,
		Instrument(
			net.JoinHostPort(addr, strconv.Itoa(port)),
			service,
			MakeRouteFinder((*testOgenServer)(nil)),
			&testMetrics{
				meterProvider:     provider.MeterProvider,
				textMapPropagator: propagation.Baggage{},
				tracerProvider:    provider.TracerProvider,
			},
		),
	)
	rw := httptest.NewRecorder()
	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: testOgenRoutePath,
		},
	}
	h.ServeHTTP(rw, req.WithContext(context.Background()))
	require.Equal(t, http.StatusOK, rw.Code)
	provider.Flush()

	otelhttpAttrs := []attribute.KeyValue{
		attribute.String("http.request.method", req.Method),
		attribute.Int("http.response.status_code", code),
		attribute.String("server.address", addr),
		attribute.Int("server.port", port),
		attribute.String("url.path", testOgenRoutePath),
		attribute.String("url.scheme", "http"),
	}
	{
		spans := provider.Exporter.GetSpans()
		require.Len(t, spans, 1)
		span := spans[0]

		// Sort/deduplicate.
		set := attribute.NewSet(span.Attributes...)
		attrs := set.ToSlice()

		require.Equal(t,
			otelhttpAttrs,
			attrs,
		)
	}
	{
		ctx := t.Context()

		// Check server metrics
		var rm metricdata.ResourceMetrics
		err := provider.Reader.Collect(ctx, &rm)
		require.NoError(t, err)

		require.Len(t, rm.ScopeMetrics, 1)
		scope := rm.ScopeMetrics[0]

		for _, m := range scope.Metrics {
			var attrs []attribute.KeyValue
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.Sum[float64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.ExponentialHistogram[int64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.ExponentialHistogram[float64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.Histogram[int64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					attrs = p.Attributes.ToSlice()
					if len(attrs) != 0 {
						break
					}
				}
			}
			t.Logf("metric: %q", m.Name)

			expectedSet := attribute.NewSet(slices.Concat(
				otelhttpAttrs,
				[]attribute.KeyValue{
					attribute.String("oas.route.name", testOgenRouteName),
					attribute.String("oas.operation.id", testOgenRouteID),
					attribute.String("oteldb.api", service),
				},
			)...)
			expectedSet, _ = expectedSet.Filter(attribute.NewDenyKeysFilter(
				"url.path",
			))
			assert.Equal(t,
				expectedSet.ToSlice(),
				attrs,
			)
		}
	}
}

func TestInstrumentation(t *testing.T) {
	provider := integration.NewProvider()
	tracer := provider.Tracer("test")
	fn := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		t.Logf("Handler(ctx).IsValid(): %v", trace.SpanContextFromContext(ctx).IsValid())
		assert.True(t, trace.SpanContextFromContext(ctx).IsValid())
		_, span := tracer.Start(r.Context(), "Handler")
		defer span.End()
		w.WriteHeader(http.StatusOK)
	})
	h := Wrap(fn,
		otelhttp.NewMiddleware("otelhttp.Middleware",
			otelhttp.WithTracerProvider(provider),
		),
		func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				sc := trace.SpanContextFromContext(ctx)
				t.Logf("after(ctx).IsValid(): %v (%s)", sc.IsValid(), sc.TraceID())
				assert.True(t, sc.IsValid(), "Middleware span should be valid")

				ctx, span := tracer.Start(ctx, "After")
				defer span.End()
				handler.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	)
	rw := httptest.NewRecorder()
	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path: testOgenRoutePath,
		},
	}
	h.ServeHTTP(rw, req.WithContext(context.Background()))
	require.Equal(t, http.StatusOK, rw.Code)
	provider.Flush()
	spans := provider.Exporter.GetSpans()
	assert.Len(t, spans, 3)
}
