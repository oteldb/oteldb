package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/otel/attribute"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
	"github.com/oteldb/oteldb/internal/profilehandler"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/tempohandler"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// setupAPIs builds every enabled query API over b. They are the same handlers and engines cmd/oteldb
// serves; only the seam underneath differs, which is the point of the split.
func (a *App) setupAPIs(b *storagebackend.Backend) error {
	if a.cfg.Prometheus.enabled() {
		if err := a.setupProm(b); err != nil {
			return errors.Wrap(err, "prometheus")
		}
	}
	if a.cfg.Loki.enabled() {
		if err := a.setupLoki(b); err != nil {
			return errors.Wrap(err, "loki")
		}
	}
	if a.cfg.Tempo.enabled() {
		if err := a.setupTempo(b); err != nil {
			return errors.Wrap(err, "tempo")
		}
	}
	if a.cfg.Pyroscope.enabled() {
		if err := a.setupPyroscope(b); err != nil {
			return errors.Wrap(err, "pyroscope")
		}
	}

	return nil
}

// addOgen registers an ogen server under name, wrapped in the standard observability middleware.
func addOgen[
	R httpmiddleware.OgenRoute,
	Server interface {
		httpmiddleware.OgenServer[R]
		http.Handler
	},
](a *App, name, bind string, server Server, extra ...httpmiddleware.Middleware) {
	routeFinder := httpmiddleware.MakeRouteFinder(server)

	middlewares := []httpmiddleware.Middleware{
		httpmiddleware.InjectLogger(a.lg.Named(name)),
		httpmiddleware.Instrument(bind, name, routeFinder, a.tel),
		httpmiddleware.LogRequests(routeFinder),
		httpmiddleware.Explain(),
	}
	middlewares = append(middlewares, extra...)

	a.servers[name] = &http.Server{
		Addr:              bind,
		Handler:           httpmiddleware.Wrap(server, middlewares...),
		ReadHeaderTimeout: 15 * time.Second,
	}
}

func (a *App) setupProm(b *storagebackend.Backend) error {
	cfg := a.cfg.Prometheus

	engine, err := promql.New(b, promql.EngineOpts{
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     cfg.EnableAtModifier,
		EnableNegativeOffset: *cfg.EnableNegativeOffset,
		EnablePerStepStats:   cfg.EnablePerStepStats,
	})
	if err != nil {
		return errors.Wrap(err, "create PromQL engine")
	}

	s, err := promapi.NewServer(promhandler.NewPromAPI(engine, b, b, b, promhandler.PromAPIOptions{}),
		promapi.WithAttributes(attribute.String("oteldb.api", "prom")),
		promapi.WithTracerProvider(a.tel.TracerProvider()),
		promapi.WithMeterProvider(a.tel.MeterProvider()),
		promapi.WithMiddleware(promhandler.TimeoutMiddleware()),
	)
	if err != nil {
		return err
	}

	addOgen(a, "prom", cfg.Bind, s, promhandler.PatchForm)

	return nil
}

func (a *App) setupLoki(b *storagebackend.Backend) error {
	q := b.Logs()

	optimizers := append(logqlengine.DefaultOptimizers(), &storagebackend.LogQLOptimizer{})

	engine, err := logqlengine.NewEngine(q, logqlengine.Options{
		ParseOptions:   logql.ParseOptions{AllowDots: true},
		Optimizers:     optimizers,
		MeterProvider:  a.tel.MeterProvider(),
		TracerProvider: a.tel.TracerProvider(),
	})
	if err != nil {
		return errors.Wrap(err, "create LogQL engine")
	}

	s, err := lokiapi.NewServer(lokihandler.NewLokiAPI(q, engine, lokihandler.LokiAPIOptions{}),
		lokiapi.WithAttributes(attribute.String("oteldb.api", "loki")),
		lokiapi.WithTracerProvider(a.tel.TracerProvider()),
		lokiapi.WithMeterProvider(a.tel.MeterProvider()),
		lokiapi.WithErrorHandler(writeLokiError),
	)
	if err != nil {
		return err
	}

	addOgen(a, "loki", a.cfg.Loki.Bind, s)

	return nil
}

// writeLokiError renders an error as the bare JSON string the Loki API returns.
func writeLokiError(_ context.Context, w http.ResponseWriter, _ *http.Request, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ogenerrors.ErrorCode(err))

	e := jx.GetEncoder()
	defer jx.PutEncoder(e)

	if err != nil {
		e.Str(err.Error())
	} else {
		e.Str("<nil>")
	}

	_, _ = w.Write(e.Bytes())
}

func (a *App) setupTempo(b *storagebackend.Backend) error {
	q := b.Traces()

	engine := traceqlengine.NewEngine(q, traceqlengine.Options{
		TracerProvider: a.tel.TracerProvider(),
	})

	s, err := tempoapi.NewServer(tempohandler.NewTempoAPI(q, engine, tempohandler.TempoAPIOptions{}),
		tempoapi.WithAttributes(attribute.String("oteldb.api", "tempo")),
		tempoapi.WithTracerProvider(a.tel.TracerProvider()),
		tempoapi.WithMeterProvider(a.tel.MeterProvider()),
	)
	if err != nil {
		return err
	}

	addOgen(a, "tempo", a.cfg.Tempo.Bind, s)

	return nil
}

func (a *App) setupPyroscope(b *storagebackend.Backend) error {
	q := b.Profiles()

	engine := profileqlengine.NewEngine(q, profileqlengine.Options{
		TracerProvider: a.tel.TracerProvider(),
	})

	s, err := pyroscopeapi.NewServer(profilehandler.NewPyroscopeAPI(q, engine, profilehandler.PyroscopeAPIOptions{}),
		pyroscopeapi.WithAttributes(attribute.String("oteldb.api", "pyroscope")),
		pyroscopeapi.WithTracerProvider(a.tel.TracerProvider()),
		pyroscopeapi.WithMeterProvider(a.tel.MeterProvider()),
	)
	if err != nil {
		return err
	}

	interceptor, err := otelconnect.NewInterceptor(
		otelconnect.WithTracerProvider(a.tel.TracerProvider()),
		otelconnect.WithMeterProvider(a.tel.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create connect interceptor")
	}

	// Grafana's built-in Pyroscope datasource speaks the connect QuerierService; it shares the
	// listener with the legacy HTTP API, routed by path prefix.
	connectPath, connectHandler := querierv1connect.NewQuerierServiceHandler(
		profilehandler.NewQuerierService(q, engine), connect.WithInterceptors(interceptor))

	addOgen(a, "pyroscope", a.cfg.Pyroscope.Bind, s, connectMount(connectPath, connectHandler))

	return nil
}

// connectMount serves h for requests under prefix, delegating everything else to the next handler.
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
