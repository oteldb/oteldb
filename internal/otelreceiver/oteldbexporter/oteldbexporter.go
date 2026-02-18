// Package oteldbexporter contains oteldb exporter factory.
package oteldbexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

const (
	typeStr   = "oteldbexporter"
	stability = component.StabilityLevelDevelopment
)

var typ = component.MustNewType(typeStr)

// NewFactory creates new factory of [Exporter].
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typ,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		DSN: "clickhouse://localhost:9000",
	}
}

func createTracesExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, settings, cfg, tracestorage.NewConsumer(inserter).ConsumeTraces)
}

func createMetricsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(ctx, settings, cfg, inserter.ConsumeMetrics)
}

func createLogsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	ecfg := cfg.(*Config)
	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, errors.Wrap(err, "connect to ClickHouse")
	}
	consumer, err := logstorage.NewConsumer(inserter, logstorage.ConsumerOptions{
		MeterProvider:  settings.MeterProvider,
		TracerProvider: settings.TracerProvider,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create consumer")
	}
	return exporterhelper.NewLogs(ctx, settings, cfg, consumer.ConsumeLogs)
}
