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
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor"
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

	"github.com/go-faster/oteldb/internal/otelreceiver/oteldbexporter"
	"github.com/go-faster/oteldb/internal/otelreceiver/prometheusremotewritereceiver"
)

func receiverFactoryMap() (map[component.Type]receiver.Factory, error) {
	return otelcol.MakeFactoryMap(
		otlpreceiver.NewFactory(),
		prometheusremotewritereceiver.NewFactory(),
	)
}

func processorFactoryMap() (map[component.Type]processor.Factory, error) {
	return otelcol.MakeFactoryMap(
		attributesprocessor.NewFactory(),
		batchprocessor.NewFactory(),
		resourceprocessor.NewFactory(),
		metricstransformprocessor.NewFactory(),
	)
}

func exporterFactoryMap() (map[component.Type]exporter.Factory, error) {
	return otelcol.MakeFactoryMap(
		oteldbexporter.NewFactory(),
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

// Factories returns oteldb factories list.
func Factories(settings TelemetrySettings) func() (f otelcol.Factories, _ error) {
	settings.setDefaults()
	return func() (f otelcol.Factories, _ error) {
		receivers, err := receiverFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get receiver factory map")
		}

		processors, err := processorFactoryMap()
		if err != nil {
			return f, errors.Wrap(err, "get processor factory map")
		}

		exporters, err := exporterFactoryMap()
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
