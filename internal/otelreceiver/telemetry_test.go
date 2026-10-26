package otelreceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTelemetryFactoryLogger(t *testing.T) {
	core, observed := observer.New(zapcore.InfoLevel)
	settings := TelemetrySettings{Logger: zap.New(core).Named("embedded").With(zap.String("source", "test"))}
	factory := telemetryFactory(settings)
	logger, shutdown, err := factory.CreateLogger(t.Context(), telemetry.LoggerSettings{}, factory.CreateDefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdown(t.Context())) })
	logger.Debug("filtered")
	logger.Info("collector")
	require.Len(t, observed.All(), 1)
	entry := observed.All()[0]
	require.Equal(t, "embedded.otelcol", entry.LoggerName)
	require.Equal(t, map[string]any{"source": "test"}, entry.ContextMap())
}
