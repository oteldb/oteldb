// Command otelproxy is a Grafana datasource proxy.
package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/httpmiddleware"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokiproxy"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
	"github.com/go-faster/oteldb/internal/pyroproxy"
	"github.com/go-faster/oteldb/internal/pyroscopeapi"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempoproxy"
)

type service struct {
	addr      string
	name      string
	handler   http.Handler
	findRoute httpmiddleware.RouteFinder
	cleanup   func() error
}

func (s service) Run(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
	if s.cleanup != nil {
		defer func() {
			if err := s.cleanup(); err != nil {
				lg.Error("Cleanup failed", zap.Error(err))
			}
		}()
	}
	httpServer := &http.Server{
		Addr:              s.addr,
		Handler:           ServiceMiddleware(s, lg, m),
		ReadHeaderTimeout: 15 * time.Second,
	}
	lg.Info("Starting HTTP server",
		zap.String("addr", s.addr),
	)
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

// ServiceMiddleware is a generic middleware for any service.
func ServiceMiddleware(s service, lg *zap.Logger, m *app.Telemetry) http.Handler {
	return httpmiddleware.Wrap(s.handler,
		httpmiddleware.InjectLogger(lg),
		httpmiddleware.Instrument(s.name, s.findRoute, m),
		httpmiddleware.LogRequests(s.findRoute),
	)
}

type services struct {
	ports map[string]service
}

func (s *services) addService(addr string, srv service) error {
	if s.ports == nil {
		s.ports = map[string]service{}
	}
	if existing, ok := s.ports[addr]; ok {
		return errors.Errorf("service conflict: %q and %q on %q", existing.name, srv.name, addr)
	}

	s.ports[addr] = srv
	return nil
}

func (s *services) Prometheus(m *app.Telemetry) error {
	const (
		prefix      = "PROMETHEUS"
		defaultPort = ":9090"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := promapi.NewClient(upstreamURL,
		promapi.WithTracerProvider(m.TracerProvider()),
		promapi.WithMeterProvider(m.MeterProvider()),
		promapi.WithClient(s.httpClient(nil, m)),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	var (
		rec     *promproxy.Recorder
		cleanup func() error
	)
	if fName := os.Getenv(prefix + "_RECORD"); fName != "" {
		// #nosec G304 G302 G703
		f, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
		if err != nil {
			return errors.Wrap(err, "create record file")
		}
		rec = promproxy.NewRecorder(f)
		cleanup = func() error {
			if err := f.Close(); err != nil {
				return errors.Wrap(err, "close record file")
			}
			return nil
		}
	}
	server, err := promapi.NewServer(
		promproxy.NewServer(client, rec),
		promapi.WithTracerProvider(m.TracerProvider()),
		promapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:      addr,
		name:      strings.ToLower(prefix),
		handler:   server,
		findRoute: httpmiddleware.MakeRouteFinder[promapi.Route](server),
		cleanup:   cleanup,
	})
}

func (s *services) Loki(m *app.Telemetry) error {
	const (
		prefix      = "LOKI"
		defaultPort = ":3100"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := lokiapi.NewClient(upstreamURL,
		lokiapi.WithTracerProvider(m.TracerProvider()),
		lokiapi.WithMeterProvider(m.MeterProvider()),
		lokiapi.WithClient(s.httpClient(nil, m)),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := lokiapi.NewServer(
		lokiproxy.NewServer(client),
		lokiapi.WithTracerProvider(m.TracerProvider()),
		lokiapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:      addr,
		name:      strings.ToLower(prefix),
		handler:   server,
		findRoute: httpmiddleware.MakeRouteFinder[lokiapi.Route](server),
	})
}

func (s *services) Pyroscope(m *app.Telemetry) error {
	const (
		prefix      = "PYROSCOPE"
		defaultPort = ":4040"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := pyroscopeapi.NewClient(upstreamURL,
		pyroscopeapi.WithTracerProvider(m.TracerProvider()),
		pyroscopeapi.WithMeterProvider(m.MeterProvider()),
		pyroscopeapi.WithClient(s.httpClient(newPyroscopeTransport, m)),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := pyroscopeapi.NewServer(
		pyroproxy.NewServer(client),
		pyroscopeapi.WithTracerProvider(m.TracerProvider()),
		pyroscopeapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:      addr,
		name:      strings.ToLower(prefix),
		handler:   server,
		findRoute: httpmiddleware.MakeRouteFinder[pyroscopeapi.Route](server),
	})
}

func (s *services) Tempo(m *app.Telemetry) error {
	const (
		prefix      = "TEMPO"
		defaultPort = ":3200"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := tempoapi.NewClient(upstreamURL,
		tempoapi.WithTracerProvider(m.TracerProvider()),
		tempoapi.WithMeterProvider(m.MeterProvider()),
		tempoapi.WithClient(s.httpClient(nil, m)),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := tempoapi.NewServer(
		tempoproxy.NewServer(client),
		tempoapi.WithTracerProvider(m.TracerProvider()),
		tempoapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:      addr,
		name:      strings.ToLower(prefix),
		handler:   server,
		findRoute: httpmiddleware.MakeRouteFinder[tempoapi.Route](server),
	})
}

func (s *services) httpClient(wrap TransportMiddleware, m *app.Telemetry) *http.Client {
	transport := http.DefaultTransport
	if wrap != nil {
		transport = wrap(transport)
	}
	return &http.Client{
		Transport: otelhttp.NewTransport(transport,
			otelhttp.WithTracerProvider(m.TracerProvider()),
			otelhttp.WithMeterProvider(m.MeterProvider()),
		),
	}
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
		s := services{}
		if err := s.Prometheus(m); err != nil {
			return errors.Wrapf(err, "setup Prometheus proxy")
		}
		if err := s.Loki(m); err != nil {
			return errors.Wrapf(err, "setup Loki proxy")
		}
		if err := s.Pyroscope(m); err != nil {
			return errors.Wrapf(err, "setup Pyroscope proxy")
		}
		if err := s.Tempo(m); err != nil {
			return errors.Wrapf(err, "setup Tempo proxy")
		}

		if len(s.ports) == 0 {
			return errors.New("no services to run")
		}
		g, ctx := errgroup.WithContext(ctx)
		for _, s := range s.ports {
			s := s
			lg := lg.Named(s.name)
			g.Go(func() error {
				return s.Run(ctx, lg, m)
			})
		}
		return g.Wait()
	})
}
