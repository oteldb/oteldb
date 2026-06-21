package tetragonreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "tetragonreceiver", factory.Type().String())

	config := factory.CreateDefaultConfig()
	assert.NotNil(t, config, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(config))
}

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig()
	config.(*Config).Endpoint = "localhost:54321"

	params := receivertest.NewNopSettings(typ)
	logsReceiver, err := factory.CreateLogs(context.Background(), params, config, consumertest.NewNop())
	assert.NoError(t, err, "Logs receiver creation failed")
	assert.NotNil(t, logsReceiver, "Receiver creation failed")

	metricReceiver, err := factory.CreateMetrics(context.Background(), params, config, consumertest.NewNop())
	assert.ErrorIs(t, err, pipeline.ErrSignalNotSupported)
	assert.Nil(t, metricReceiver)
}

func TestFactoryType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, typeStr, factory.Type().String())
}

func TestCreateDefaultConfigType(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig()
	_, ok := config.(*Config)
	require.True(t, ok, "Config must be of type *Config")
}
