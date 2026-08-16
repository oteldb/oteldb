package main

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// App is the odbingest process: an embedded storage engine and the ingest endpoints writing into
// it. It owns the engine, so shutting it down flushes what has not reached a part yet.
type App struct {
	cfg        Config
	lg         *zap.Logger
	closeStore func(context.Context) error
	srv        *http.Server
}

func newApp(ctx context.Context, cfg Config, lg *zap.Logger, m *app.Telemetry) (*App, error) {
	backend, closeStore, err := storagebackend.Open(ctx, cfg.Storage, lg.Named("storage"), m)
	if err != nil {
		return nil, errors.Wrap(err, "open storage")
	}

	obs, err := newObserver(m.MeterProvider())
	if err != nil {
		_ = closeStore(ctx)
		return nil, errors.Wrap(err, "create metrics")
	}

	rw := cfg.RemoteWrite
	handler := promrw.NewHandler(backend, promrw.HandlerConfig{
		Options: promrw.Options{
			TimeThreshold: rw.TimeThreshold,
		},
		MaxBodyBytes:    int64(rw.MaxBodyBytes),
		MaxDecodedBytes: int(rw.MaxDecodedBytes),
		Logger:          lg.Named("remotewrite"),
		Observer:        obs.observe,
	})

	mux := http.NewServeMux()
	mux.Handle(rw.Path, handler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	return &App{
		cfg:        cfg,
		lg:         lg,
		closeStore: closeStore,
		srv: &http.Server{
			Addr:              rw.Bind,
			Handler:           mux,
			ReadHeaderTimeout: rw.ReadHeaderTimeout,
		},
	}, nil
}

// Run serves until ctx is canceled, then drains in-flight writes and flushes the engine.
func (a *App) Run(ctx context.Context) error {
	a.lg.Info("Serving Prometheus remote write",
		zap.String("bind", a.cfg.RemoteWrite.Bind),
		zap.String("path", a.cfg.RemoteWrite.Path),
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.Wrap(err, "serve remote write")
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()

		// Stop accepting, let in-flight writes finish: they hold the engine's ingest path, and a
		// write cut off mid-flight is a write the sender must retry.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.RemoteWrite.ShutdownTimeout)
		defer cancel()

		return a.srv.Shutdown(shutdownCtx)
	})

	err := g.Wait()

	// The engine closes after the server, so nothing writes into a closing engine. Its own context
	// must outlive the canceled one: closing is what flushes the head to a part.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.RemoteWrite.ShutdownTimeout)
	defer cancel()

	a.lg.Info("Flushing storage engine")
	if closeErr := a.closeStore(closeCtx); closeErr != nil {
		return errors.Join(err, errors.Wrap(closeErr, "close storage"))
	}
	return err
}
