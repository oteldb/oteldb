// Package hareceiver implements an OpenTelemetry Collector receiver that polls
// journal logs from a Home Assistant instance and emits OTLP logs.
package hareceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr   = "hareceiver"
	stability = component.StabilityLevelDevelopment

	defaultPollInterval = 10 * time.Second
	defaultBatchSize    = 1000

	// defaultRecombineWindow is far above the observed p99 gap between
	// fragments of one message, and far below the gap between unrelated
	// entries from the same process.
	defaultRecombineWindow = time.Second
)

var typ = component.MustNewType(typeStr)

// NewFactory creates new factory of [Receiver].
func NewFactory() receiver.Factory {
	return receiver.NewFactory(typ, createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, stability))
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig:    confighttp.NewDefaultClientConfig(),
		PollInterval:    defaultPollInterval,
		BatchSize:       defaultBatchSize,
		ParseMessage:    true,
		RecombineWindow: defaultRecombineWindow,
	}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	lc consumer.Logs,
) (receiver.Logs, error) {
	return NewReceiver(params, cfg.(*Config), lc)
}
