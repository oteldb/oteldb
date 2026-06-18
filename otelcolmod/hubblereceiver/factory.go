package hubblereceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr   = "hubblereceiver"
	stability = component.StabilityLevelDevelopment
)

var typ = component.MustNewType(typeStr)

// NewFactory creates new factory of [Receiver].
func NewFactory() receiver.Factory {
	return receiver.NewFactory(typ, createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, stability))
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	lc consumer.Logs,
) (receiver.Logs, error) {
	return NewReceiver(params, cfg.(*Config), lc)
}
