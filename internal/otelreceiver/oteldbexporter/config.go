package oteldbexporter

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/exporter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/chstorage"
)

// Config defines [Exporter] config.
type Config struct {
	DSN        string        `mapstructure:"dsn"`
	CHLogLevel zapcore.Level `mapstructure:"ch_log_level"`
}

func (c *Config) connect(ctx context.Context, settings exporter.Settings) (*chstorage.Inserter, error) {
	pool, err := chstorage.Dial(ctx, c.DSN, chstorage.DialOptions{
		MeterProvider:  settings.MeterProvider,
		TracerProvider: settings.TracerProvider,
		Logger:         settings.Logger.Named("ch").WithOptions(zap.IncreaseLevel(c.CHLogLevel)),
	})
	if err != nil {
		return nil, errors.Wrap(err, "dial clickhouse")
	}

	inserter, err := chstorage.NewInserter(pool, chstorage.InserterOptions{
		Tables:         chstorage.DefaultTables(),
		CHLogLevel:     c.CHLogLevel,
		MeterProvider:  settings.MeterProvider,
		TracerProvider: settings.TracerProvider,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create inserter")
	}

	return inserter, nil
}
