package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	sdkapp "github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/otelcol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chembed"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/httpmiddleware"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promhandler"
	"github.com/go-faster/oteldb/internal/promql"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
)

// App contains application dependencies and services.
type App struct {
	cfg Config

	services map[string]func(context.Context) error
	shutdown func()
	otelStorage

	lg        *zap.Logger
	telemetry *sdkapp.Telemetry
}

func newApp(ctx context.Context, cfg Config, m *sdkapp.Telemetry) (_ *App, err error) {
	cfg.setDefaults()

	app := &App{
		cfg:       cfg,
		services:  map[string]func(context.Context) error{},
		lg:        zctx.From(ctx),
		telemetry: m,
	}

	{
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

		var (
			routeFinder = httpmiddleware.MakeRouteFinder(server)
			middlewares = []httpmiddleware.Middleware{
				httpmiddleware.InjectLogger(zctx.From(ctx)),
				httpmiddleware.Instrument("oteldb."+name, routeFinder, app.telemetry),
				httpmiddleware.LogRequests(routeFinder),
			}
		)

		auth, err := makeAuthMiddlewares(authCfg)
		if err != nil {
			return errors.Wrap(err, "create auth middlewares")
		}
		if auth != nil {
			lg.Info("Enabling authentication middleware", zap.Int("configs", len(authCfg)))
			middlewares = append(middlewares, auth)
		}

		httpServer := &http.Server{
			Addr:              addr,
			Handler:           httpmiddleware.Wrap(server, middlewares...),
			ReadHeaderTimeout: 15 * time.Second,
		}

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

func makeAuthMiddlewares(auth []AuthConfig) (httpmiddleware.Middleware, error) {
	if len(auth) == 0 {
		return nil, nil
	}

	r := make([]httpmiddleware.Authenticator, 0, len(auth))
	for _, a := range auth {
		if !a.Type.IsValid() {
			return nil, errors.Errorf("invalid auth type %q", a.Type)
		}

		a.setDefaults()
		switch a.Type {
		case AuthTypeBasic:
			r = append(r, httpmiddleware.BasicAuth(a.Users))
		case AuthTypeBearerToken:
			r = append(r, httpmiddleware.BearerToken(a.Tokens))
		}
	}
	return httpmiddleware.Auth(r, nil), nil
}

func (app *App) trySetupTempo() error {
	q := app.traceQuerier
	if q == nil {
		return nil
	}
	cfg := app.cfg.Tempo
	cfg.setDefaults()

	engine := traceqlengine.NewEngine(app.traceQuerier, traceqlengine.Options{
		TracerProvider: app.telemetry.TracerProvider(),
	})
	tempo := tempohandler.NewTempoAPI(q, engine, tempohandler.TempoAPIOptions{})

	s, err := tempoapi.NewServer(tempo,
		tempoapi.WithTracerProvider(app.telemetry.TracerProvider()),
		tempoapi.WithMeterProvider(app.telemetry.MeterProvider()),
	)
	if err != nil {
		return err
	}

	addOgen(app, "tempo", s, cfg.Bind, cfg.Auth)
	return nil
}

func (app *App) trySetupLoki() error {
	q := app.logQuerier
	if q == nil {
		return nil
	}
	cfg := app.cfg.Loki
	cfg.setDefaults()

	var optimizers []logqlengine.Optimizer
	optimizers = append(optimizers, logqlengine.DefaultOptimizers()...)
	optimizers = append(optimizers, &chstorage.ClickhouseOptimizer{})
	engine, err := logqlengine.NewEngine(q, logqlengine.Options{
		ParseOptions: logql.ParseOptions{
			AllowDots: true,
		},
		LookbackDuration: cfg.LookbackDelta,
		Optimizers:       optimizers,
		MeterProvider:    app.telemetry.MeterProvider(),
		TracerProvider:   app.telemetry.TracerProvider(),
	})
	if err != nil {
		return errors.Wrap(err, "create LogQL engine")
	}
	loki := lokihandler.NewLokiAPI(q, engine)

	s, err := lokiapi.NewServer(loki,
		lokiapi.WithTracerProvider(app.telemetry.TracerProvider()),
		lokiapi.WithMeterProvider(app.telemetry.MeterProvider()),
		lokiapi.WithErrorHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
			code := ogenerrors.ErrorCode(err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)

			e := jx.GetEncoder()
			defer jx.PutEncoder(e)

			if err != nil {
				e.Str(err.Error())
			} else {
				e.Str("<nil>")
			}

			_, _ = w.Write(e.Bytes())
		}),
	)
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
	cfg.setDefaults()

	engine, err := promql.New(q, promql.EngineOpts{
		// NOTE: zero-value MaxSamples and Timeout makes
		// all queries to fail with error.
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
	prom := promhandler.NewPromAPI(engine, q, q, promhandler.PromAPIOptions{})

	s, err := promapi.NewServer(prom,
		promapi.WithTracerProvider(app.telemetry.TracerProvider()),
		promapi.WithMeterProvider(app.telemetry.MeterProvider()),
		promapi.WithMiddleware(promhandler.TimeoutMiddleware()),
	)
	if err != nil {
		return err
	}

	addOgen(app, "prom", s, cfg.Bind, cfg.Auth)
	return nil
}

func (app *App) setupHealthCheck() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/readiness", app.handleReadinessProbe)
	mux.HandleFunc("/liveness", app.handleLivenessProbe)
	mux.HandleFunc("/startup", app.handleStartupProbe)
	var handler http.Handler = mux

	cfg := app.cfg.HealthCheck
	cfg.setDefaults()

	auth, err := makeAuthMiddlewares(cfg.Auth)
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

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		Factories: otelreceiver.Factories(telemetry),
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
		s := s
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
