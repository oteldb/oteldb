package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	sdkapp "github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/chembed"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otelreceiver"
	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/queryapi"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// App contains application dependencies and services.
type App struct {
	cfg Config

	services map[string]func(context.Context) error
	shutdown func()
	otelStorage

	// metricsSink/tracesSink/logsSink, when set, route the corresponding signal's ingestion to
	// the embedded storage engine instead of ClickHouse (see [MetricsBackendStorage]).
	metricsSink  otelreceiver.MetricsSink
	tracesSink   otelreceiver.TracesSink
	logsSink     otelreceiver.LogsSink
	profilesSink otelreceiver.ProfilesSink

	// storageBackend is the embedded storage engine, set when any signal is served from it. It backs
	// the admin panel's first-class storage view. Nil when every signal is on ClickHouse.
	storageBackend *storagebackend.Backend

	// tenancy resolves each query request's tenant from its credential. Nil when read-path tenancy
	// is not configured, in which case every read serves the default tenant.
	tenancy httpmiddleware.Middleware

	lg        *zap.Logger
	telemetry *sdkapp.Telemetry
	startTime time.Time
}

func newApp(ctx context.Context, cfg Config, m *sdkapp.Telemetry) (_ *App, err error) {
	cfg.setDefaults()

	app := &App{
		cfg:       cfg,
		services:  map[string]func(context.Context) error{},
		lg:        zctx.From(ctx),
		telemetry: m,
		startTime: time.Now(),
	}

	if err := cfg.validateTenancy(); err != nil {
		return nil, err
	}

	tenancy, err := config.TenancyMiddleware(cfg.Tenancy)
	if err != nil {
		return nil, errors.Wrap(err, "setup tenancy")
	}
	app.tenancy = tenancy

	// ClickHouse is started only when a queryable signal is still served by it. Under --embedded
	// (every signal on the embedded storage engine) ClickHouse is skipped entirely, including the
	// zero-config embedded ClickHouse, and no DSN is required.
	if cfg.needsClickHouse() {
		dsn := os.Getenv("CH_DSN")
		if dsn == "" {
			dsn = cfg.DSN
		}
		if dsn == "" {
			// Embedded ClickHouse mode.
			app.lg.Info("Starting embedded ClickHouse")
			dsn = "clickhouse://default:@localhost:9000/default?debug=true"
			err := chembed.New(ctx, app.lg.Named("clickhouse"))
			if err != nil {
				return nil, errors.Wrap(err, "start embedded clickhouse")
			}
			app.lg.Info("Embedded ClickHouse started")
		}

		switch replicated := os.Getenv("CH_REPLICATED"); strings.ToLower(replicated) {
		case "y", "yes", "t", "true", "on", "1":
			cfg.Replicated = true
		case "n", "no", "f", "false", "off", "0":
			cfg.Replicated = false
		}
		if cluster := os.Getenv("CH_CLUSTER"); cluster != "" {
			cfg.Cluster = cluster
		}

		store, err := setupCH(ctx, dsn, cfg, app.lg, m)
		if err != nil {
			return nil, errors.Wrapf(err, "create storage")
		}
		app.otelStorage = store
	} else {
		app.lg.Info("ClickHouse disabled; serving all signals from the embedded storage engine")
	}

	// Optionally swap one or more signals onto the embedded storage engine. A single shared
	// engine instance backs every signal selected via the *_backend config; the rest stay on
	// ClickHouse. For each swapped signal both the query side (the API handler's querier) and
	// the ingestion side (the collector exporter's sink) are replaced.
	if cfg.usesStorageBackend() {
		var storageOpts []storagebackend.Option
		if cfg.Tenancy.Enabled {
			storageOpts = append(storageOpts, storagebackend.WithTenancy())
		}

		b, closeStore, err := storagebackend.Open(
			ctx, cfg.Storage, app.lg.Named("storage"), app.telemetry, storageOpts...,
		)
		if err != nil {
			return nil, errors.Wrap(err, "setup storage backend")
		}
		app.storageBackend = b
		if cfg.MetricsBackend == MetricsBackendStorage {
			app.metricsQuerier = b
			app.metricsSink = b
		}
		if cfg.TracesBackend == MetricsBackendStorage {
			app.traceQuerier = b.Traces()
			app.tracesSink = b
		}
		if cfg.LogsBackend == MetricsBackendStorage {
			app.logQuerier = b.Logs()
			app.logsSink = b
		}
		if cfg.ProfilesBackend == MetricsBackendStorage {
			app.profileQuerier = b.Profiles()
			app.profilesSink = b
		}
		app.services["storage"] = func(ctx context.Context) error {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := closeStore(shutdownCtx); err != nil {
				return errors.Wrap(err, "close storage")
			}
			return nil
		}
	}

	if err := app.setupHealthCheck(); err != nil {
		return nil, errors.Wrap(err, "healthcheck")
	}
	if err := app.setupCollector(); err != nil {
		return nil, errors.Wrap(err, "otelcol")
	}
	if err := app.trySetupTempo(); err != nil {
		return nil, errors.Wrap(err, "tempo")
	}
	if err := app.trySetupLoki(); err != nil {
		return nil, errors.Wrap(err, "loki")
	}
	if err := app.trySetupProm(); err != nil {
		return nil, errors.Wrap(err, "prometheus")
	}
	if err := app.trySetupPyroscope(); err != nil {
		return nil, errors.Wrap(err, "pyroscope")
	}
	if err := app.setupAdmin(); err != nil {
		return nil, errors.Wrap(err, "admin")
	}

	return app, nil
}

func addOgen[
	R httpmiddleware.OgenRoute,
	Server interface {
		httpmiddleware.OgenServer[R]
		http.Handler
	},
](
	app *App,
	name string,
	server Server,
	defaultPort string,
	authCfg []AuthConfig,
	additionalMiddlewares ...httpmiddleware.Middleware,
) {
	lg := app.lg.Named(name)

	addr := os.Getenv(strings.ToUpper(name) + "_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	if authCfg == nil {
		authCfg = app.cfg.Auth
	}

	app.services[name] = func(ctx context.Context) error {
		lg := lg.With(zap.String("addr", addr))
		lg.Info("Starting HTTP server")

		var middlewares []httpmiddleware.Middleware
		auth, err := config.AuthMiddleware(authCfg)
		if err != nil {
			return errors.Wrap(err, "create auth middlewares")
		}
		if auth != nil {
			lg.Info("Enabling authentication middleware", zap.Int("configs", len(authCfg)))
			middlewares = append(middlewares, auth)
		}
		// Tenancy sits inside authentication: it resolves which tenants a credential reads, while an
		// outer authenticator only decides whether the request gets that far.
		if app.tenancy != nil {
			middlewares = append(middlewares, app.tenancy)
		}
		middlewares = append(middlewares, additionalMiddlewares...)

		httpServer := queryapi.HTTPServer(queryapi.ServerOptions{
			Name:    name,
			Addr:    addr,
			Logger:  zctx.From(ctx),
			Metrics: app.telemetry,
		}, server, middlewares...)

		parentCtx := ctx
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			<-ctx.Done()
			lg.Info("Shutting down")

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return httpServer.Shutdown(ctx)
		})
		g.Go(func() error {
			if err := httpServer.ListenAndServe(); err != nil {
				if errors.Is(err, http.ErrServerClosed) && parentCtx.Err() != nil {
					lg.Info("HTTP server closed gracefully")
					return nil
				}
				return errors.Wrap(err, "http server")
			}
			return nil
		})
		return g.Wait()
	}
}

func (app *App) trySetupTempo() error {
	q := app.traceQuerier
	if q == nil {
		return nil
	}
	cfg := app.cfg.Tempo
	cfg.SetDefaults()

	s, err := queryapi.NewTempo(queryapi.TempoOptions{
		Querier:        q,
		TracerProvider: app.telemetry.TracerProvider(),
		MeterProvider:  app.telemetry.MeterProvider(),
	})
	if err != nil {
		return err
	}

	addOgen(app, "tempo", s, cfg.Bind, cfg.Auth)
	return nil
}

func (app *App) trySetupPyroscope() error {
	q := app.profileQuerier
	if q == nil {
		// Profiles storage backend is not wired in yet (deferred to
		// oteldb/storage); skip the Pyroscope API.
		return nil
	}
	cfg := app.cfg.Pyroscope
	cfg.SetDefaults()

	s, connectMount, err := queryapi.NewPyroscope(queryapi.PyroscopeOptions{
		Querier:        q,
		TracerProvider: app.telemetry.TracerProvider(),
		MeterProvider:  app.telemetry.MeterProvider(),
	})
	if err != nil {
		return err
	}

	addOgen(app, "pyroscope", s, cfg.Bind, cfg.Auth, connectMount)
	return nil
}

func (app *App) trySetupLoki() error {
	q := app.logQuerier
	if q == nil {
		return nil
	}
	cfg := app.cfg.Loki
	cfg.SetDefaults()

	// The ClickHouse optimizer pushes filtering into chstorage's InputNode; it is a no-op for
	// other backends, so only enable it when logs are actually served from ClickHouse. When logs
	// are served from the embedded storage engine, the storage optimizer offloads line filters into
	// the fetch instead.
	var optimizer logqlengine.Optimizer = &chstorage.ClickhouseOptimizer{}
	if app.cfg.LogsBackend == MetricsBackendStorage {
		optimizer = &storagebackend.LogQLOptimizer{}
	}

	s, err := queryapi.NewLoki(queryapi.LokiOptions{
		Config:         cfg,
		Querier:        q,
		Optimizers:     []logqlengine.Optimizer{optimizer},
		TracerProvider: app.telemetry.TracerProvider(),
		MeterProvider:  app.telemetry.MeterProvider(),
	})
	if err != nil {
		return err
	}

	addOgen(app, "loki", s, cfg.Bind, cfg.Auth)
	return nil
}

func (app *App) trySetupProm() error {
	q := app.metricsQuerier
	if q == nil {
		return nil
	}
	cfg := app.cfg.Prometheus
	cfg.SetDefaults()

	s, err := queryapi.NewPrometheus(queryapi.PrometheusOptions{
		Config:         cfg,
		Querier:        q,
		Logger:         app.lg,
		TracerProvider: app.telemetry.TracerProvider(),
		MeterProvider:  app.telemetry.MeterProvider(),
	})
	if err != nil {
		return err
	}

	addOgen(app, "prom", s, cfg.Bind, cfg.Auth, promhandler.PatchForm)
	return nil
}

// newScarecrowEngine builds the internal/scarecrow engine for [App.trySetupProm].
func (app *App) newScarecrowEngine(q metricQuerier, cfg PrometheusConfig) *scarecrow.Engine {
	var tracerProvider trace.TracerProvider
	// Telemetry is absent in tests that build an App directly; scarecrow falls back to the global
	// provider when this is unset.
	if app.telemetry != nil {
		tracerProvider = app.telemetry.TracerProvider()
	}

	return queryapi.NewScarecrowEngine(app.lg, q, cfg, tracerProvider)
}

func (app *App) setupHealthCheck() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/readiness", app.handleReadinessProbe)
	mux.HandleFunc("/liveness", app.handleLivenessProbe)
	mux.HandleFunc("/startup", app.handleStartupProbe)
	var handler http.Handler = mux

	cfg := app.cfg.HealthCheck
	cfg.SetDefaults()

	auth, err := config.AuthMiddleware(cfg.Auth)
	if err != nil {
		return errors.Wrap(err, "create auth middlewares")
	}
	if auth != nil {
		app.lg.Info("Enabling healthcheck authentication middleware", zap.Int("configs", len(cfg.Auth)))
		handler = httpmiddleware.Wrap(handler, auth)
	}

	srv := &http.Server{
		Addr:              cfg.Bind,
		Handler:           handler,
		ReadHeaderTimeout: time.Second,
	}
	app.services["healthcheck"] = func(ctx context.Context) error {
		go func() {
			<-ctx.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		}()
		if err := srv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
				zctx.From(ctx).Info("Healthcheck HTTP server closed gracefully")
				return nil
			}
			return errors.Wrap(err, "healthcheck http server")
		}
		return nil
	}
	return nil
}

func (app *App) handleReadinessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (app *App) handleLivenessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (app *App) handleStartupProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (app *App) setupCollector() error {
	var telemetry otelreceiver.TelemetrySettings
	{
		sig := app.cfg.CollectorSignals
		if sig["logs"] {
			telemetry.Logger = app.lg
			telemetry.LoggerProvider = app.telemetry.LoggerProvider()
		}
		if sig["metrics"] {
			telemetry.MeterProvider = app.telemetry.MeterProvider()
		}
		if sig["trace"] {
			telemetry.TracerProvider = app.telemetry.TracerProvider()
		}
	}

	var factoryOpts []otelreceiver.Option
	if app.metricsSink != nil {
		factoryOpts = append(factoryOpts, otelreceiver.WithMetricsSink(app.metricsSink))
	}
	if app.tracesSink != nil {
		factoryOpts = append(factoryOpts, otelreceiver.WithTracesSink(app.tracesSink))
	}
	if app.logsSink != nil {
		factoryOpts = append(factoryOpts, otelreceiver.WithLogsSink(app.logsSink))
	}
	if app.profilesSink != nil {
		factoryOpts = append(factoryOpts, otelreceiver.WithProfilesSink(app.profilesSink))
		// The collector gates its experimental profiles pipeline behind a feature gate; enable it
		// so the profiles signal (served from the embedded storage engine) can be ingested.
		if err := featuregate.GlobalRegistry().Set("service.profilesSupport", true); err != nil {
			return errors.Wrap(err, "enable profiles support feature gate")
		}
	}

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		Factories: otelreceiver.Factories(telemetry, factoryOpts...),
		BuildInfo: component.NewDefaultBuildInfo(),
		LoggingOptions: []zap.Option{
			zap.WrapCore(func(zapcore.Core) zapcore.Core {
				return app.lg.Core()
			}),
		},
		DisableGracefulShutdown: false,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{"oteldb:/"},
				ProviderFactories: []confmap.ProviderFactory{
					confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
						return otelreceiver.NewMapProvider("oteldb", app.cfg.Collector)
					}),
					envprovider.NewFactory(),
				},
			},
		},
		SkipSettingGRPCLogger: false,
	})
	if err != nil {
		return errors.Wrap(err, "create collector")
	}

	app.services["otelcol"] = func(ctx context.Context) error {
		// Collector is listening for os.Interrupt, syscall.SIGTERM itself,
		// and will return nil error on shutdown. See Collector.Run.
		//
		// So, we should shut down other services.
		defer app.shutdown()

		return col.Run(ctx)
	}
	return nil
}

// Run runs application.
func (app *App) Run(ctx context.Context) error {
	ctx, app.shutdown = context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	runningServices := make(map[string]struct{})
	var runningServicesMux sync.Mutex
	for k := range app.services {
		runningServices[k] = struct{}{}
	}

	for k, s := range app.services {
		g.Go(func() (rerr error) {
			defer func() {
				if r := recover(); r != nil {
					rerr = errors.New("panic recovered")
					zctx.From(ctx).Error("panic", zap.Any("panic", r))
				}
			}()
			defer func() {
				zctx.From(ctx).Debug("Service shut down",
					zap.Error(rerr),
					zap.String("service_key", k),
				)
				runningServicesMux.Lock()
				delete(runningServices, k)
				runningServicesMux.Unlock()
			}()
			return s(ctx)
		})
	}
	g.Go(func() error {
		<-ctx.Done()
		zctx.From(ctx).Debug("Application is shutting down")
		ticker := time.NewTicker(time.Second * 5)
		go func() {
			defer ticker.Stop()
			for range ticker.C {
				runningServicesMux.Lock()
				running := maps.Keys(runningServices)
				runningServicesMux.Unlock()

				zctx.From(ctx).Debug("Still shutting down",
					zap.Strings("running_services", running),
				)
			}
		}()
		return nil
	})
	return g.Wait()
}
