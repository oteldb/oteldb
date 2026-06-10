package odbsafetyprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const stability = component.StabilityLevelDevelopment

// NewFactory creates a new odbsafety processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typ,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, stability),
	)
}

func createLogsProcessor(
	_ context.Context,
	settings processor.Settings,
	cfg component.Config,
	next consumer.Logs,
) (processor.Logs, error) {
	c := cfg.(*Config)
	p, err := newLogsProcessor(settings, c, next)
	if err != nil {
		return nil, err
	}
	return p, nil
}
