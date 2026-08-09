package hareceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	require.Equal(t, typ, f.Type())
	require.Equal(t, stability, f.LogsStability())
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.Equal(t, defaultPollInterval, cfg.PollInterval)
	require.Equal(t, defaultBatchSize, cfg.BatchSize)
	require.False(t, cfg.SeverityFromMessage)
	require.Nil(t, cfg.StorageID)
	require.Error(t, cfg.Validate(), "default config is incomplete")
}

func TestCreateLogsReceiver(t *testing.T) {
	f := NewFactory()

	t.Run("Valid", func(t *testing.T) {
		r, err := f.CreateLogs(
			context.Background(),
			receivertest.NewNopSettings(typ),
			testConfig(nil),
			consumertest.NewNop(),
		)
		require.NoError(t, err)
		require.IsType(t, (*Receiver)(nil), r)
	})
	t.Run("Invalid", func(t *testing.T) {
		_, err := f.CreateLogs(
			context.Background(),
			receivertest.NewNopSettings(typ),
			testConfig(func(c *Config) { c.Token = "" }),
			consumertest.NewNop(),
		)
		require.ErrorContains(t, err, "token is required")
	})
}
