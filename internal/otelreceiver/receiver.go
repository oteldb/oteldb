// Package otelreceiver provides simple wrapper to setup trace receiver.
package otelreceiver

import (
	"github.com/go-faster/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/countconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/exceptionsconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/roundrobinconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/sumconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/otelreceiver/oteldbexporter"
	"github.com/oteldb/oteldb/otelcolmod/hubblereceiver"
	"github.com/oteldb/oteldb/otelcolmod/odbsafetyprocessor"
	"github.com/oteldb/oteldb/otelcolmod/prometheusremotewritereceiver"
	"github.com/oteldb/oteldb/otelcolmod/tetragonreceiver"
)

func receiverFactoryMap() (map[component.Type]receiver.Factory, error) {
	return otelcol.MakeFactoryMap(
		otlpreceiver.NewFactory(),
		prometheusremotewritereceiver.NewFactory(),
		hubblereceiver.NewFactory(),
		tetragonreceiver.NewFactory(),
	)
}

func processorFactoryMap() (map[component.Type]processor.Factory, error) {
	return otelcol.MakeFactoryMap(
		attributesprocessor.NewFactory(),
		batchprocessor.NewFactory(),
		filterprocessor.NewFactory(),
		transformprocessor.NewFactory(),
		resourceprocessor.NewFactory(),
		metricstransformprocessor.NewFactory(),
		odbsafetyprocessor.NewFactory(),
	)
}

func exporterFactoryMap(opts ...oteldbexporter.Option) (map[component.Type]exporter.Factory, error) {
	return otelcol.MakeFactoryMap(
		oteldbexporter.NewFactory(opts...),
	)
}

func extensionFactoryMap() (map[component.Type]extension.Factory, error) {
	return otelcol.MakeFactoryMap(
		basicauthextension.NewFactory(),
		bearertokenauthextension.NewFactory(),
		oidcauthextension.NewFactory(),
	)
}

func connectorFactoryMap() (map[component.Type]connector.Factory, error) {
	return otelcol.MakeFactoryMap(
		countconnector.NewFactory(),
		exceptionsconnector.NewFactory(),
		failoverconnector.NewFactory(),
		servicegraphconnector.NewFactory(),
		sumconnector.NewFactory(),
		signaltometricsconnector.NewFactory(),
		roundrobinconnector.NewFactory(),
	)
}

// TelemetrySettings provides telemetry for collector.
type TelemetrySettings struct {
	Logger         *zap.Logger
	LoggerProvider log.LoggerProvider
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
}

func (s *TelemetrySettings) setDefaults() {
	if s.Logger == nil {
		s.Logger = zap.NewNop()
	}
	if s.LoggerProvider == nil {
		s.LoggerProvider = lognoop.NewLoggerProvider()
	}
	if s.MeterProvider == nil {
		s.MeterProvider = metricnoop.NewMeterProvider()
	}
	if s.TracerProvider == nil {
		s.TracerProvider = tracenoop.NewTracerProvider()
	}
}

// MetricsSink ingests OTLP metrics batches into an alternative backend (the embedded
// storage engine). See [WithMetricsSink].
type MetricsSink = oteldbexporter.MetricsSink

// TracesSink ingests OTLP traces batches into an alternative backend (the embedded
// storage engine). See [WithTracesSink].
type TracesSink = oteldbexporter.TracesSink

// LogsSink ingests OTLP logs batches into an alternative backend (the embedded
// storage engine). See [WithLogsSink].
type LogsSink = oteldbexporter.LogsSink

// ProfilesSink ingests OTLP profiles batches into an alternative backend (the embedded
// storage engine). See [WithProfilesSink].
type ProfilesSink = oteldbexporter.ProfilesSink

// Option configures [Factories].
type Option func(*factoriesOptions)

type factoriesOptions struct {
	exporterOpts []oteldbexporter.Option
}

// WithMetricsSink routes ingested metrics to sink instead of ClickHouse.
func WithMetricsSink(sink MetricsSink) Option {
	return func(o *factoriesOptions) {
		o.exporterOpts = append(o.exporterOpts, oteldbexporter.WithMetricsSink(sink))
	}
}

// WithTracesSink routes ingested traces to sink instead of ClickHouse.
func WithTracesSink(sink TracesSink) Option {
	return func(o *factoriesOptions) {
		o.exporterOpts = append(o.exporterOpts, oteldbexporter.WithTracesSink(sink))
	}
}

// WithLogsSink routes ingested logs to sink instead of ClickHouse.
func WithLogsSink(sink LogsSink) Option {
	return func(o *factoriesOptions) {
		o.exporterOpts = append(o.exporterOpts, oteldbexporter.WithLogsSink(sink))
	}
}

// WithProfilesSink enables the profiles signal, routing ingested profiles to sink.
func WithProfilesSink(sink ProfilesSink) Option {
	return func(o *factoriesOptions) {
		o.exporterOpts = append(o.exporterOpts, oteldbexporter.WithProfilesSink(sink))
	}
}

// Factories returns oteldb factories list.
func Factories(settings TelemetrySettings, opts ...Option) func() (f otelcol.Factories, _ error) {
	settings.setDefaults()
	var fo factoriesOptions
	for _, o := range opts {
		o(&fo)
	}
	return func() (f otelcol.Factories, _ error) {
		receivers, err := receiverFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get receiver factory map")
		}

		processors, err := processorFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get processor factory map")
		}

		exporters, err := exporterFactoryMap(fo.exporterOpts...)
		if err != nil {
			return f, errors.Wrap(err, "get exporter factory map")
		}

		extensions, err := extensionFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get extension factory map")
		}

		connectors, err := connectorFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get connector factory map")
		}

		return otelcol.Factories{
			Receivers:  receivers,
			Processors: processors,
			Exporters:  exporters,
			Extensions: extensions,
			Connectors: connectors,
			Telemetry:  telemetryFactory(settings),
		}, nil
	}
}
