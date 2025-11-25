package otelreceiver

import (
	"context"

	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func telemetryFactory(lg *zap.Logger, tel *app.Telemetry) telemetry.Factory {
	return telemetry.NewFactory(
		func() component.Config { return new(telemetryConfig) },
		telemetry.WithCreateLogger(func(ctx context.Context, ls telemetry.LoggerSettings, c component.Config) (*zap.Logger, component.ShutdownFunc, error) {
			return lg.Named("otelcol").WithOptions(ls.ZapOptions...), func(ctx context.Context) error { return nil }, nil
		}),
		telemetry.WithCreateMeterProvider(func(ctx context.Context, ms telemetry.MeterSettings, c component.Config) (telemetry.MeterProvider, error) {
			return &meterProvider{tel.MeterProvider()}, nil
		}),
		telemetry.WithCreateTracerProvider(func(ctx context.Context, ts telemetry.TracerSettings, c component.Config) (telemetry.TracerProvider, error) {
			return &tracerProvider{tel.TracerProvider()}, nil
		}),
	)
}

type telemetryConfig struct{}

type meterProvider struct {
	metric.MeterProvider
}

var _ telemetry.MeterProvider = (*meterProvider)(nil)

// Shutdown implements [telemetry.MeterProvider].
func (*meterProvider) Shutdown(context.Context) error { return nil }

type tracerProvider struct {
	trace.TracerProvider
}

var _ telemetry.TracerProvider = (*tracerProvider)(nil)

// Shutdown implements [telemetry.TracerProvider].
func (*tracerProvider) Shutdown(context.Context) error { return nil }
