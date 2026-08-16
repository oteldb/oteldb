package main

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/storage/cluster/router"

	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/promrw"
)

// App is the odbingest process: the ingest endpoints and a routing view of the cluster they write
// into. It holds no data — everything it accepts is durable in the cluster before the request is
// answered, so shutting it down only has to let in-flight writes finish.
type App struct {
	cfg    Config
	lg     *zap.Logger
	router *router.Router
	srv    *http.Server
}

func newApp(ctx context.Context, cfg Config, lg *zap.Logger, m *app.Telemetry) (*App, error) {
	rt, err := router.Open(ctx, router.Config{
		Etcd:            cfg.Cluster.Etcd,
		Root:            cfg.Cluster.Root,
		RF:              cfg.Cluster.RF,
		ShardsPerTenant: cfg.Cluster.ShardsPerTenant,
		DialTimeout:     cfg.Cluster.DialTimeout,
		Logger:          lg.Named("cluster"),
	})
	if err != nil {
		return nil, errors.Wrap(err, "open cluster router")
	}

	sink, err := newClusterSink(rt, nil, m.MeterProvider())
	if err != nil {
		_ = rt.Close(ctx)

		return nil, errors.Wrap(err, "create sink")
	}

	obs, err := newObserver(m.MeterProvider())
	if err != nil {
		_ = rt.Close(ctx)

		return nil, errors.Wrap(err, "create metrics")
	}

	otlp := otlpdirect.NewHandler(sink, otlpdirect.HandlerConfig{
		MaxBodyBytes:    int64(cfg.OTLP.MaxBodyBytes),
		MaxDecodedBytes: int64(cfg.OTLP.MaxDecodedBytes),
		Logger:          lg.Named("otlp"),
		Observer:        obs.observeOTLP,
	})

	rw := cfg.RemoteWrite
	handler := promrw.NewHandler(sink, promrw.HandlerConfig{
		Options: promrw.Options{
			TimeThreshold: rw.TimeThreshold,
		},
		MaxBodyBytes:    int64(rw.MaxBodyBytes),
		MaxDecodedBytes: int(rw.MaxDecodedBytes),
		Logger:          lg.Named("remotewrite"),
		Observer:        obs.observe,
	})

	mux := http.NewServeMux()
	otlp.Register(mux)
	mux.Handle(rw.Path, handler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /readyz", readyHandler(rt))

	return &App{
		cfg:    cfg,
		lg:     lg,
		router: rt,
		srv: &http.Server{
			Addr:              rw.Bind,
			Handler:           mux,
			ReadHeaderTimeout: rw.ReadHeaderTimeout,
		},
	}, nil
}

// readyHandler reports ready once the ring has a member to route to. Accepting a write against an
// empty ring can only fail it, and answering 503 instead keeps a starting pod out of the load
// balancer until the cluster is actually reachable.
func readyHandler(rt *router.Router) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if len(rt.Members()) == 0 {
			http.Error(w, "no cluster members", http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

// Run serves until ctx is canceled, then drains in-flight writes.
func (a *App) Run(ctx context.Context) error {
	a.lg.Info("Serving ingest",
		zap.String("bind", a.cfg.RemoteWrite.Bind),
		zap.String("remote_write_path", a.cfg.RemoteWrite.Path),
		zap.Strings("otlp_paths", []string{
			otlpdirect.LogsPath, otlpdirect.TracesPath, otlpdirect.MetricsPath, otlpdirect.ProfilesPath,
		}),
		zap.Strings("etcd", a.cfg.Cluster.Etcd),
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

		// Stop accepting, let in-flight writes finish: a write cut off mid-flight is one the
		// sender must retry, and the cluster may already have taken it.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.RemoteWrite.ShutdownTimeout)
		defer cancel()

		return a.srv.Shutdown(shutdownCtx)
	})

	err := g.Wait()

	// The router closes after the server, so nothing routes through a closing membership watch.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.RemoteWrite.ShutdownTimeout)
	defer cancel()

	if closeErr := a.router.Close(closeCtx); closeErr != nil {
		return errors.Join(err, errors.Wrap(closeErr, "close router"))
	}

	return err
}
