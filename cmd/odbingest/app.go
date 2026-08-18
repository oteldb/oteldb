package main

import (
	"context"
	"net"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"

	"github.com/oteldb/storage/cluster"
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
	grpc   *grpc.Server
}

func newApp(ctx context.Context, cfg Config, lg *zap.Logger, m *app.Telemetry) (*App, error) {
	rt, err := router.Open(ctx, cfg.Cluster.RouterConfig(lg.Named("cluster")))
	if err != nil {
		return nil, errors.Wrap(err, "open cluster router")
	}

	tenants, err := newTenantResolver(cfg.Tenant)
	if err != nil {
		_ = rt.Close(ctx)

		return nil, errors.Wrap(err, "configure tenancy")
	}

	// Which tenant a write lands in decides which shard, hence which node, holds it. An operator
	// reading a startup log must be able to tell whether this process resolves tenants at all.
	if tenants == nil {
		lg.Info("Tenancy disabled, routing every write to the default tenant",
			zap.String("tenant", string(cluster.DefaultTenant)))
	} else {
		lg.Info("Tenancy enabled",
			zap.String("header", tenants.header),
			zap.Strings("resource_attributes", cfg.Tenant.ResourceAttributes),
			zap.String("default", string(tenants.defaultTenant())),
			zap.Bool("require", tenants.required),
		)
	}

	sink, err := newClusterSink(rt, tenants.tenantFunc, m.MeterProvider())
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

	// Only the ingest routes are wrapped: a health probe carries no tenant, and requiring one of it
	// would fail readiness on a correctly configured deployment.
	ingestMux := http.NewServeMux()
	otlp.Register(ingestMux)
	ingestMux.Handle(rw.Path, handler)

	mux := http.NewServeMux()
	mux.Handle("/", tenants.Middleware(ingestMux))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /readyz", readyHandler(rt))

	var grpcSrv *grpc.Server
	if cfg.OTLP.GRPCBind != "-" {
		opts := append(otlp.GRPCServerOptions(), grpc.ChainUnaryInterceptor(tenants.UnaryInterceptor()))
		grpcSrv = grpc.NewServer(opts...)
		otlp.RegisterGRPC(grpcSrv)
	}

	return &App{
		cfg:    cfg,
		lg:     lg,
		router: rt,
		grpc:   grpcSrv,
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
		zap.String("otlp_grpc_bind", a.cfg.OTLP.GRPCBind),
		zap.Strings("etcd", a.cfg.Cluster.Etcd),
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.Wrap(err, "serve http")
		}

		return nil
	})

	if a.grpc != nil {
		var lc net.ListenConfig

		ln, err := lc.Listen(ctx, "tcp", a.cfg.OTLP.GRPCBind)
		if err != nil {
			return errors.Wrap(err, "listen for otlp grpc")
		}

		g.Go(func() error {
			if err := a.grpc.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				return errors.Wrap(err, "serve otlp grpc")
			}

			return nil
		})
		g.Go(func() error {
			<-gctx.Done()

			// GracefulStop lets in-flight exports finish; a write cut off mid-flight is one the
			// exporter must retry, and the cluster may already have taken it.
			a.grpc.GracefulStop()

			return nil
		})
	}
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
