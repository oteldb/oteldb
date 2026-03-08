package oteldbexporter

import (
	"context"
	"math/rand/v2"

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
	Traces     TracesConfig  `mapstructure:"traces"`
	Metrics    MetricsConfig `mapstructure:"metrics"`
	Logs       LogsConfig    `mapstructure:"logs"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if err := c.Traces.Spans.validate("traces.spans"); err != nil {
		return err
	}
	if err := c.Metrics.Exemplars.validate("metrics.exemplars"); err != nil {
		return err
	}
	if err := c.Logs.Records.validate("logs.records"); err != nil {
		return err
	}
	return nil
}

// SamplingConfig defines sampling options for a signal or sub-signal.
type SamplingConfig struct {
	// Drop drops all items before inserting. Takes precedence over Rate.
	Drop bool `mapstructure:"drop"`
	// Rate is the fraction of items to keep, in (0.0, 1.0).
	// Zero (default) disables sampling and all items are kept.
	Rate float64 `mapstructure:"rate"`
}

func (s SamplingConfig) validate(field string) error {
	if s.Rate < 0 || s.Rate > 1 {
		return errors.Errorf("%s: rate must be in [0.0, 1.0], got %f", field, s.Rate)
	}
	return nil
}

// enabled reports whether any sampling is configured.
func (s SamplingConfig) enabled() bool {
	return s.Drop || s.Rate > 0
}

// keep reports whether the current item should be kept.
func (s SamplingConfig) keep() bool {
	if s.Drop {
		return false
	}
	if s.Rate <= 0 || s.Rate >= 1 {
		return true
	}
	return rand.Float64() < s.Rate //#nosec G404
}

// TracesConfig defines options for traces consumer.
type TracesConfig struct {
	// Spans controls sampling of individual spans.
	Spans SamplingConfig `mapstructure:"spans"`
}

// MetricsConfig defines options for metrics consumer.
type MetricsConfig struct {
	// Exemplars controls sampling of exemplars attached to data points.
	Exemplars SamplingConfig `mapstructure:"exemplars"`
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

// LogsConfig defines options for logs consumer.
type LogsConfig struct {
	// Records controls sampling of individual log records.
	Records    SamplingConfig       `mapstructure:"records"`
	Processing LogsProcessingConfig `mapstructure:"processing"`
}

// LogsProcessingConfig defines options for logs preprocessing.
type LogsProcessingConfig struct {
	// TriggerAttributes defines a list of attribute references to trigger format detection.
	TriggerAttributes []string `mapstructure:"trigger_attributes"`
	// FormatAttributes defines a list of attribute references to choose format.
	//
	// Format attribute may define multple formats via comma.
	// The first valid would be used to parse the record.
	//
	// If attribute value is absent, have an unknown or invalid value, the record would be passed as-is.
	FormatAttributes []string `mapstructure:"format_attributes"`
	// DetectFormats defines list of formats to detect.
	DetectFormats []string `mapstructure:"formats"`
}
