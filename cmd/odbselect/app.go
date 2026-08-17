package main

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	sdkapp "github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/storage/cluster/router"

	"github.com/oteldb/oteldb/internal/clusterquery"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// App is the odbselect process: the query APIs and a routing view of the cluster they read from. It
// holds no data — every answer comes from a shard owner — so shutting it down only has to let
// in-flight queries finish.
type App struct {
	cfg    Config
	lg     *zap.Logger
	tel    *sdkapp.Telemetry
	router *router.Router

	servers map[string]*http.Server
}

func newApp(ctx context.Context, cfg Config, lg *zap.Logger, m *sdkapp.Telemetry) (*App, error) {
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

	app := &App{
		cfg:     cfg,
		lg:      lg,
		tel:     m,
		router:  rt,
		servers: map[string]*http.Server{},
	}

	backend := storagebackend.NewQuery(clusterquery.New(rt, nil))

	if err := app.setupAPIs(backend); err != nil {
		_ = rt.Close(ctx)

		return nil, err
	}

	app.servers["health"] = &http.Server{
		Addr:              cfg.Health.Bind,
		Handler:           healthMux(rt),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return app, nil
}

// healthMux serves liveness and readiness. Readiness is gated on the ring having a member to read
// from: answering a query against an empty ring can only produce an empty result that looks like an
// answer, and 503 instead keeps a starting pod out of the load balancer until the cluster is
// actually reachable.
func healthMux(rt *router.Router) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /readyz", readinessHandler(func() int { return len(rt.Members()) }))

	return mux
}

func readinessHandler(members func() int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if members() == 0 {
			http.Error(w, "no cluster members", http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

// Run serves until ctx is canceled, then drains in-flight queries.
func (a *App) Run(ctx context.Context) error {
	for name, srv := range a.servers {
		a.lg.Info("Serving", zap.String("api", name), zap.String("bind", srv.Addr))
	}

	g, gctx := errgroup.WithContext(ctx)

	for name, srv := range a.servers {
		g.Go(func() error {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return errors.Wrapf(err, "serve %s", name)
			}

			return nil
		})
		g.Go(func() error {
			<-gctx.Done()

			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.ShutdownTimeout)
			defer cancel()

			return srv.Shutdown(shutdownCtx)
		})
	}

	err := g.Wait()

	// The router closes after the servers, so nothing reads through a closing membership watch.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.ShutdownTimeout)
	defer cancel()

	if closeErr := a.router.Close(closeCtx); closeErr != nil {
		return errors.Join(err, errors.Wrap(closeErr, "close router"))
	}

	return err
}
