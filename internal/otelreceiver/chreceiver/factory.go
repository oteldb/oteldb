// Package chreceiver implements a ClickHouse OpenTelemetry receiver.
package chreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/oteldb/oteldb/internal/chotel"
)

const (
	typeStr   = "chreceiver"
	stability = component.StabilityLevelDevelopment
)

var typ = component.MustNewType(typeStr)

// NewFactory creates new factory of [Receiver].
func NewFactory() receiver.Factory {
	return receiver.NewFactory(typ, createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, stability))
}

func createDefaultConfig() component.Config {
	return &Config{
		DSN:      "clickhouse://default:@localhost:9000/default",
		PollRate: defaultPollRate,
		Lag:      chotel.DefaultLag,
		Lookback: chotel.DefaultLookback,
	}
}

func createTracesReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	tconsumer consumer.Traces,
) (receiver.Traces, error) {
	rCfg := cfg.(*Config)
	return NewReceiver(params, rCfg, tconsumer)
}
