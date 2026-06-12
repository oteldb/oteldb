// Binary chotel exports clichkouse traces to otel collector.
package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oteldb/oteldb/internal/chotel"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) (err error) {
		a, err := NewApp(lg, m)
		if err != nil {
			return errors.Wrap(err, "init")
		}
		ctx = zctx.WithOpenTelemetryZap(ctx)
		return a.Run(ctx)
	},
		app.WithServiceName("oteldb.chotel"),
	)
}

// App is the trace exporter application.
type App struct {
	log     *zap.Logger
	metrics *app.Telemetry

	clickHouseAddr     string
	clickHousePassword string
	clickHouseUser     string
	clickHouseDB       string

	otlpAddr string
	rate     time.Duration

	spansSaved    metric.Int64Counter
	reader        *chotel.Reader
	clickHouse    *ch.Client
	traceExporter *otlptrace.Exporter
}

// NewApp initializes the trace exporter application.
func NewApp(lg *zap.Logger, metrics *app.Telemetry) (*App, error) {
	a := &App{
		log:                lg,
		metrics:            metrics,
		clickHouseAddr:     "clickhouse:9000",
		clickHouseUser:     "default",
		clickHousePassword: "",
		clickHouseDB:       "default",
		otlpAddr:           "otelcol:4317",
		rate:               time.Millisecond * 500,
	}
	if v := os.Getenv("CHOTEL_SEND_RATE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, errors.Wrap(err, "parse CHOTEL_SEND_RATE")
		}
		a.rate = d
	}
	if v := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); v != "" {
		a.otlpAddr = strings.TrimPrefix(v, "http://")
	}
	if v := os.Getenv("CH_DSN"); v != "" {
		u, err := url.Parse(v)
		if err != nil {
			return nil, errors.Wrap(err, "parse DSN")
		}
		a.clickHouseAddr = u.Host
		if auth := u.User; auth != nil {
			if user := auth.Username(); user != "" {
				a.clickHouseUser = user
			}
			if pass, ok := auth.Password(); ok {
				a.clickHousePassword = pass
			}
		}
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			a.clickHouseDB = db
		}
	}
	{
		meter := metrics.MeterProvider().Meter("chotel")
		var err error
		if a.spansSaved, err = meter.Int64Counter("chotel.spans.saved"); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Run starts and runs the application.
func (a *App) Run(ctx context.Context) error {
	ctx = zctx.WithOpenTelemetryZap(ctx)
	ctx = zctx.Base(ctx, a.log)
	if err := a.setup(ctx); err != nil {
		return errors.Wrap(err, "setup")
	}
	defer func() {
		if a.clickHouse != nil {
			_ = a.clickHouse.Close()
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.runSender(ctx) })
	return g.Wait()
}

func (a *App) setup(ctx context.Context) error {
	a.log.Info("Setup")
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	a.log.Info("Connecting to clickhouse")
	db, err := ch.Dial(ctx, ch.Options{
		Address:  a.clickHouseAddr,
		User:     a.clickHouseUser,
		Password: a.clickHousePassword,
		Database: a.clickHouseDB,

		OpenTelemetryInstrumentation: false,
	})
	if err != nil {
		return errors.Wrap(err, "clickhouse")
	}
	if err := db.Ping(ctx); err != nil {
		_ = db.Close()
		return errors.Wrap(err, "clickhouse ping")
	}
	a.log.Info("Connected to clickhouse")
	a.clickHouse = db
	a.reader = chotel.NewReader(db)

	conn, err := grpc.NewClient(a.otlpAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithMeterProvider(a.metrics.MeterProvider()),
		)),
	)
	if err != nil {
		return errors.Wrap(err, "dial otlp")
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return errors.Wrap(err, "setup trace exporter")
	}
	a.traceExporter = traceExporter
	a.log.Info("Ready to export traces")
	return nil
}

func (a *App) send(ctx context.Context, now time.Time) error {
	spans, err := a.reader.Read(ctx, now)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	batch := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		batch = append(batch, chotel.ConvertTrace(span))
	}
	eb := backoff.NewExponentialBackOff()
	if err := backoff.Retry(func() error {
		if err := a.traceExporter.ExportSpans(ctx, batch); err != nil {
			return errors.Wrap(err, "export")
		}
		return nil
	}, eb); err != nil {
		return errors.Wrap(err, "export")
	}
	a.reader.Advance(chotel.MaxFinishTime(spans))
	zctx.From(ctx).Info("Exported", zap.Int("count", len(spans)))
	return nil
}

func (a *App) runSender(ctx context.Context) error {
	ticker := time.NewTicker(a.rate)
	defer ticker.Stop()

	// First immediate tick.
	if err := a.send(ctx, time.Now()); err != nil {
		return errors.Wrap(err, "send")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if err := a.send(ctx, now); err != nil {
				return errors.Wrap(err, "send")
			}
		}
	}
}
