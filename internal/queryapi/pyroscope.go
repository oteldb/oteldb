package queryapi

import (
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/go-faster/errors"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/profilehandler"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

// PyroscopeOptions configures the Pyroscope API server.
type PyroscopeOptions struct {
	// Querier serves the API.
	Querier profilestorage.Querier
	// TracerProvider and MeterProvider instrument the server and the engine.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// NewPyroscope builds the Pyroscope API server and the middleware serving the connect
// QuerierService, which Grafana's built-in Pyroscope datasource speaks, on the same listener as
// the legacy Pyroscope HTTP API.
func NewPyroscope(opts PyroscopeOptions) (*pyroscopeapi.Server, httpmiddleware.Middleware, error) {
	engine := profileqlengine.NewEngine(opts.Querier, profileqlengine.Options{
		TracerProvider: opts.TracerProvider,
	})
	handler := profilehandler.NewPyroscopeAPI(opts.Querier, engine, profilehandler.PyroscopeAPIOptions{})

	s, err := pyroscopeapi.NewServer(handler,
		pyroscopeapi.WithAttributes(attribute.String("oteldb.api", "pyroscope")),
		pyroscopeapi.WithTracerProvider(opts.TracerProvider),
		pyroscopeapi.WithMeterProvider(opts.MeterProvider),
	)
	if err != nil {
		return nil, nil, err
	}

	interceptor, err := otelconnect.NewInterceptor(
		otelconnect.WithTracerProvider(opts.TracerProvider),
		otelconnect.WithMeterProvider(opts.MeterProvider),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create connect interceptor")
	}
	querier := profilehandler.NewQuerierService(opts.Querier, engine)
	connectPath, connectHandler := querierv1connect.NewQuerierServiceHandler(querier, connect.WithInterceptors(interceptor))

	return s, connectMount(connectPath, connectHandler), nil
}

// connectMount returns a middleware that serves the connect handler h for requests whose path is
// under prefix, delegating everything else to the next handler.
func connectMount(prefix string, h http.Handler) httpmiddleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, prefix) {
				h.ServeHTTP(w, r)

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
