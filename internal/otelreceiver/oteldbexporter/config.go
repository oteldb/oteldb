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

// ProcessingConfig defines options for telemetry pre-processing.
type ProcessingConfig struct {
	// TriggerAttributes defines a list of attribute references to trigger format detection.
	TriggerAttributes []string
	// FormatAttributes defines a list of attribute references to choose format.
	//
	// Format attribute may define multple formats via comma.
	// The first valid would be used to parse the record.
	//
	// If attribute value is absent, have an unknown or invalid value, the record would be passed as-is.
	FormatAttributes []string
	// DetectFormats defines list of formats to detect.
	DetectFormats []string
}
