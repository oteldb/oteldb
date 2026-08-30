package main

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	sdkapp "github.com/go-faster/sdk/app"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/storage/cluster/router"

	"github.com/oteldb/oteldb/internal/adminapi"
	"github.com/oteldb/oteldb/internal/adminhandler"
	"github.com/oteldb/oteldb/internal/cliversion"
	"github.com/oteldb/oteldb/internal/clusteradmin"
	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/queryapi"
)

// App is the odbadmin process: an aggregated admin API over the cluster's member nodes, and the
// same web UI a storage node serves. It holds no data — every number comes from a member — so
// shutting it down only has to let in-flight requests finish.
type App struct {
	cfg    Config
	lg     *zap.Logger
	router *router.Router

	servers map[string]*http.Server
}

func newApp(ctx context.Context, cfg Config, lg *zap.Logger, m *sdkapp.Telemetry) (*App, error) {
	rt, err := router.Open(ctx, cfg.Cluster.RouterConfig(lg.Named("cluster"), m.TracerProvider()))
	if err != nil {
		return nil, errors.Wrap(err, "open cluster router")
	}

	app := &App{
		cfg:     cfg,
		lg:      lg,
		router:  rt,
		servers: map[string]*http.Server{},
	}

	if err := app.setupAdmin(m); err != nil {
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

// setupAdmin wires the aggregating admin API and the embedded web UI.
func (a *App) setupAdmin(m *sdkapp.Telemetry) error {
	peers, err := clusteradmin.NewRingPeers(a.router, a.cfg.Nodes.Scheme, a.cfg.Nodes.Port)
	if err != nil {
		return errors.Wrap(err, "resolve node admin endpoints")
	}

	build, _ := cliversion.GetInfo("github.com/oteldb/oteldb")

	handler, err := clusteradmin.New(clusteradmin.Options{
		Peers: peers,
		Info: clusteradmin.BuildInfo{
			Version:   build.Version,
			Commit:    build.Commit,
			GoVersion: build.GoVersion,
		},
		StartTime:         time.Now(),
		ReplicationFactor: a.cfg.Cluster.RF,
		Timeout:           a.cfg.Nodes.Timeout,
		Logger:            a.lg.Named("clusteradmin"),
	})
	if err != nil {
		return errors.Wrap(err, "create cluster admin handler")
	}

	srv, err := adminapi.NewServer(handler,
		adminapi.WithAttributes(attribute.String("oteldb.api", "admin")),
		adminapi.WithTracerProvider(m.TracerProvider()),
		adminapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create admin server")
	}

	// The SPA is the one a storage node serves, embedded in adminhandler and served from there
	// rather than copied: a second copy would drift from the API it is generated against.
	middlewares := []httpmiddleware.Middleware{adminhandler.UIMiddleware()}

	auth, err := config.AuthMiddleware(a.cfg.Admin.Auth)
	if err != nil {
		return errors.Wrap(err, "build admin auth")
	}
	if auth != nil {
		middlewares = append(middlewares, auth)
	}

	a.servers["admin"] = queryapi.HTTPServer(queryapi.ServerOptions{
		Name:    "admin",
		Addr:    a.cfg.Admin.Bind,
		Logger:  a.lg.Named("admin"),
		Metrics: m,
	}, srv, middlewares...)

	return nil
}

// healthMux serves liveness and readiness. Readiness is gated on the ring having a member: with no
// member to ask, every aggregate is an empty report that looks like an answer, and 503 keeps a
// starting pod out of the load balancer until the cluster is actually reachable.
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

// Run serves until ctx is canceled, then drains in-flight requests.
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
