package chstorage

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DialOptions is [Dial] function options.
type DialOptions struct {
	// MeterProvider provides OpenTelemetry meter for pool.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for pool.
	TracerProvider trace.TracerProvider
	// Logger provides logger for pool.
	Logger *zap.Logger
}

func (opts *DialOptions) setDefaults() {
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
}

// Dial creates new [ClickHouseClient] using given DSN.
func Dial(ctx context.Context, dsn string, opts DialOptions) (ClickHouseClient, error) {
	opts.setDefaults()
	lg := opts.Logger

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "parse DSN")
	}
	lg.Debug("Dial Clickhouse", zap.String("dsn", dsn))

	pass, _ := u.User.Password()
	chLogger := lg.Named("ch")
	{
		var lvl zapcore.Level
		if v := os.Getenv("CH_LOG_LEVEL"); v != "" {
			if err := lvl.Set(v); err != nil {
				return nil, errors.Wrap(err, "parse log level")
			}
		} else {
			lvl = lg.Level()
		}
		chLogger = chLogger.WithOptions(zap.IncreaseLevel(lvl))
	}
	chOpts := ch.Options{
		Logger:         chLogger,
		Address:        u.Host,
		Database:       strings.TrimPrefix(u.Path, "/"),
		User:           u.User.Username(),
		Password:       pass,
		MeterProvider:  opts.MeterProvider,
		TracerProvider: opts.TracerProvider,

		// Capture query body and other parameters.
		OpenTelemetryInstrumentation: true,
	}

	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	return backoff.RetryNotifyWithData(
		func() (ClickHouseClient, error) {
			client, err := chpool.Dial(ctx, chpool.Options{
				ClientOptions:     chOpts,
				HealthCheckPeriod: time.Second,
				MaxConnIdleTime:   time.Second * 10,
				MaxConnLifetime:   time.Minute,
			})
			if err != nil {
				return nil, errors.Wrap(err, "dial")
			}
			if err := client.Ping(ctx); err != nil {
				return nil, errors.Wrap(err, "ping")
			}
			return client, nil
		},
		backoff.WithContext(connectBackoff, ctx),
		func(err error, d time.Duration) {
			lg.Warn("Clickhouse dial failed",
				zap.Error(err),
				zap.Duration("retry_after", d),
			)
		},
	)
}
