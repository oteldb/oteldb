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
			meter := opts.MeterProvider.Meter("chstorage.pool")
			if err := poolMetrics(meter, client); err != nil {
				return nil, errors.Wrap(err, "setup pool metrics")
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

func poolMetrics(meter metric.Meter, pool *chpool.Pool) error {
	totalResources, err := meter.Int64ObservableGauge("chpool.total_resources",
		metric.WithDescription("Total number of resources currently in the ClickHouse pool"),
		metric.WithUnit("{resources}"),
	)
	if err != nil {
		return err
	}

	constructingResources, err := meter.Int64ObservableGauge("chpool.constructing_resources",
		metric.WithDescription("Number of resources being constructed in the ClickHouse pool"),
		metric.WithUnit("{resources}"),
	)
	if err != nil {
		return err
	}

	acquiredResources, err := meter.Int64ObservableGauge("chpool.acquired_resources",
		metric.WithDescription("Number of currently acquired resources from the ClickHouse pool"),
		metric.WithUnit("{resources}"),
	)
	if err != nil {
		return err
	}

	idleResources, err := meter.Int64ObservableGauge("chpool.idle_resources",
		metric.WithDescription("Number of currently idle resources in the ClickHouse pool"),
		metric.WithUnit("{resources}"),
	)
	if err != nil {
		return err
	}

	maxResources, err := meter.Int64ObservableGauge("chpool.max_resources",
		metric.WithDescription("Maximum configured size of the ClickHouse pool"),
		metric.WithUnit("{resources}"),
	)
	if err != nil {
		return err
	}

	acquireCount, err := meter.Int64ObservableGauge("chpool.acquire_count",
		metric.WithDescription("Cumulative count of successful acquires from the ClickHouse pool"),
		metric.WithUnit("{acquires}"),
	)
	if err != nil {
		return err
	}

	acquireDuration, err := meter.Int64ObservableGauge("chpool.acquire_duration_ns",
		metric.WithDescription("Total duration of successful acquires from the ClickHouse pool in nanoseconds"),
		metric.WithUnit("ns"),
	)
	if err != nil {
		return err
	}

	emptyAcquireCount, err := meter.Int64ObservableGauge("chpool.empty_acquire_count",
		metric.WithDescription("Cumulative count of acquires that waited because the pool was empty"),
		metric.WithUnit("{acquires}"),
	)
	if err != nil {
		return err
	}

	emptyAcquireWaitTime, err := meter.Int64ObservableGauge("chpool.empty_acquire_wait_time_ns",
		metric.WithDescription("Cumulative time waited for acquires that waited because the pool was empty, in nanoseconds"),
		metric.WithUnit("ns"),
	)
	if err != nil {
		return err
	}

	canceledAcquireCount, err := meter.Int64ObservableGauge("chpool.canceled_acquire_count",
		metric.WithDescription("Cumulative count of acquires canceled by context"),
		metric.WithUnit("{acquires}"),
	)
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		stat := pool.Stat()

		o.ObserveInt64(totalResources, int64(stat.TotalResources()))
		o.ObserveInt64(constructingResources, int64(stat.ConstructingResources()))
		o.ObserveInt64(acquiredResources, int64(stat.AcquiredResources()))
		o.ObserveInt64(idleResources, int64(stat.IdleResources()))
		o.ObserveInt64(maxResources, int64(stat.MaxResources()))

		o.ObserveInt64(acquireCount, stat.AcquireCount())
		o.ObserveInt64(acquireDuration, stat.AcquireDuration().Nanoseconds())

		o.ObserveInt64(emptyAcquireCount, stat.EmptyAcquireCount())
		o.ObserveInt64(emptyAcquireWaitTime, stat.EmptyAcquireWaitTime().Nanoseconds())
		o.ObserveInt64(canceledAcquireCount, stat.CanceledAcquireCount())

		return nil
	},
		totalResources,
		constructingResources,
		acquiredResources,
		idleResources,
		maxResources,
		acquireCount,
		acquireDuration,
		emptyAcquireCount,
		emptyAcquireWaitTime,
		canceledAcquireCount,
	)

	return err
}
