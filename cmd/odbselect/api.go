package main

import (
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// setupAPIs builds every enabled query API over b. They are the same handlers and engines cmd/oteldb
// serves; only the seam underneath differs, which is the point of the split.
func (a *App) setupAPIs(b *storagebackend.Backend) error {
	if cfg := a.cfg.Prometheus; enabled(cfg.Bind) {
		s, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{
			Config:         cfg,
			Querier:        b,
			Logger:         a.lg,
			TracerProvider: a.tel.TracerProvider(),
			MeterProvider:  a.tel.MeterProvider(),
		})
		if err != nil {
			return errors.Wrap(err, "prometheus")
		}

		serve(a, "prom", cfg.Bind, s, promhandler.PatchForm)
	}
	if cfg := a.cfg.Loki; enabled(cfg.Bind) {
		s, err := queryapi.NewLoki(queryapi.LokiOptions{
			Config:         cfg,
			Querier:        b.Logs(),
			Optimizers:     []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
			TracerProvider: a.tel.TracerProvider(),
			MeterProvider:  a.tel.MeterProvider(),
		})
		if err != nil {
			return errors.Wrap(err, "loki")
		}

		serve(a, "loki", cfg.Bind, s)
	}
	if cfg := a.cfg.Tempo; enabled(cfg.Bind) {
		s, err := queryapi.NewTempo(queryapi.TempoOptions{
			Querier:        b.Traces(),
			TracerProvider: a.tel.TracerProvider(),
			MeterProvider:  a.tel.MeterProvider(),
		})
		if err != nil {
			return errors.Wrap(err, "tempo")
		}

		serve(a, "tempo", cfg.Bind, s)
	}
	if cfg := a.cfg.Pyroscope; enabled(cfg.Bind) {
		s, connectMount, err := queryapi.NewPyroscope(queryapi.PyroscopeOptions{
			Querier:        b.Profiles(),
			TracerProvider: a.tel.TracerProvider(),
			MeterProvider:  a.tel.MeterProvider(),
		})
		if err != nil {
			return errors.Wrap(err, "pyroscope")
		}

		serve(a, "pyroscope", cfg.Bind, s, connectMount)
	}

	return nil
}

// serve registers an ogen server under name. odbselect has no separate auth block and no
// <NAME>_ADDR override: binds come from the config alone, and the only credential check is the
// tenancy resolver's, when tenancy is configured.
func serve[
	R httpmiddleware.OgenRoute,
	S interface {
		httpmiddleware.OgenServer[R]
		http.Handler
	},
](a *App, name, bind string, server S, extra ...httpmiddleware.Middleware) {
	middlewares := extra
	if a.tenancy != nil {
		middlewares = append([]httpmiddleware.Middleware{a.tenancy}, extra...)
	}

	a.servers[name] = queryapi.HTTPServer(queryapi.ServerOptions{
		Name:    name,
		Addr:    bind,
		Logger:  a.lg.Named(name),
		Metrics: a.tel,
	}, server, middlewares...)
}
