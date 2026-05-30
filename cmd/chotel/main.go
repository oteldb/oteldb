// Binary chotel exports clichkouse traces to otel collector.
package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oteldb/oteldb/internal/chtrace"
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

	latest time.Time
	rate   time.Duration

	spansSaved    metric.Int64Counter
	traceExporter *otlptrace.Exporter
}

const DDL = `CREATE TABLE IF NOT EXISTS opentelemetry_span_export
(
    trace_id    UUID,
    span_id     UInt64,
    exported_at DATETIME
)
    ENGINE = MergeTree
        ORDER BY (toStartOfMinute(exported_at), trace_id, span_id)
        TTL toStartOfMinute(exported_at) + INTERVAL 10 MINUTE
`

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
	defer func() {
		_ = db.Close()
	}()
	if err := db.Ping(ctx); err != nil {
		return errors.Wrap(err, "clickhouse ping")
	}
	a.log.Info("Connected to clickhouse")
	if err := db.Do(ctx, ch.Query{Body: DDL}); err != nil {
		return errors.Wrap(err, "ensure db")
	}

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

func noPropagation(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

func (a *App) send(ctx context.Context, now time.Time) error {
	db, err := ch.Dial(noPropagation(ctx), ch.Options{
		Address:     a.clickHouseAddr,
		Compression: ch.CompressionZSTD,
		User:        a.clickHouseUser,
		Password:    a.clickHousePassword,
		Database:    a.clickHouseDB,

		OpenTelemetryInstrumentation: false,
	})
	if err != nil {
		return errors.Wrap(err, "clickhouse")
	}
	defer func() { _ = db.Close() }()

	t := chtrace.NewTable()
	q := fmt.Sprintf("SELECT %s FROM system.opentelemetry_span_log log ", strings.Join(t.Columns(), ", "))
	q += " ANTI JOIN opentelemetry_span_export ose ON log.trace_id = ose.trace_id AND log.span_id = ose.span_id"
	if !a.latest.IsZero() {
		q += fmt.Sprintf(" PREWHERE start_time_us > %d", a.latest.Add(time.Minute).UnixMilli())
	}
	q += " ORDER BY log.start_time_us DESC LIMIT 10000"
	zctx.From(ctx).Debug("Selecting spans",
		zap.String("query", q),
		zap.Time("time", a.latest),
	)
	var (
		batch    []tracesdk.ReadOnlySpan
		exported struct {
			TraceID    proto.ColUUID
			SpanID     proto.ColUInt64
			ExportedAt proto.ColDateTime
		}
	)
	clickhouseResource, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("clickhouse"),
		),
	)
	if err != nil {
		return errors.Wrap(err, "clickhouse resource")
	}
	var latest time.Time
	if err := db.Do(noPropagation(ctx), ch.Query{
		Body:   q,
		Result: t.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			exported.TraceID = append(exported.TraceID, t.TraceID...)
			exported.SpanID = append(exported.SpanID, t.SpanID...)
			for r := range t.Rows() {
				exported.ExportedAt.Append(now)
				stub := tracetest.SpanStub{
					SpanKind:  r.Kind,
					Resource:  clickhouseResource,
					Name:      r.OperationName,
					StartTime: r.StartTime,
					EndTime:   r.FinishTime,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: r.TraceID,
						SpanID:  r.SpanID,
					}),
					Parent: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: r.TraceID,
						SpanID:  r.ParentSpanID,
					}),
				}
			Attribute:
				for k, v := range r.Attributes {
					for _, marker := range []string{
						"count",
						"thread_id",
						"_bytes",
						"_rows",
						"memory_usage",
						"thread_num",
						"exception_code",
					} {
						if !strings.Contains(k, marker) {
							continue
						}
						n, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							break
						}
						stub.Attributes = append(stub.Attributes, attribute.Int64(k, n))
						continue Attribute
					}
					stub.Attributes = append(stub.Attributes, attribute.String(k, v))
				}
				if latest.Before(stub.EndTime) {
					latest = stub.EndTime
				}
				batch = append(batch, stub.Snapshot())
			}
			return nil
		},
	}); err != nil {
		return errors.Wrap(err, "query")
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
	if err := db.Do(noPropagation(ctx), ch.Query{
		Body: "INSERT INTO opentelemetry_span_export (trace_id, span_id, exported_at) VALUES",
		Input: proto.Input{
			{Name: "trace_id", Data: exported.TraceID},
			{Name: "span_id", Data: exported.SpanID},
			{Name: "exported_at", Data: exported.ExportedAt},
		},
	}); err != nil {
		return errors.Wrap(err, "insert")
	}
	a.latest = latest
	zctx.From(ctx).Info("Exported",
		zap.Int("count", len(exported.TraceID)),
		zap.String("latest_time", a.latest.String()),
	)
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
