// Package odbsafety provides an oteldb log safety stanza operator.
package odbsafety

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"go.opentelemetry.io/collector/component"

	safetyconfig "github.com/oteldb/oteldb/odbagent/internal/odbsafety"
)

// Type is the stanza operator type registered by this package.
const Type = "odbsafety"

func init() {
	RegisterDefault()
}

// NewConfig creates a new odbsafety config with default values.
func NewConfig() *Config {
	return NewConfigWithID(Type)
}

// NewConfigWithID creates a new odbsafety config with default values.
func NewConfigWithID(operatorID string) *Config {
	return &Config{
		TransformerConfig: helper.NewTransformerConfig(operatorID, Type),
		Config:            safetyconfig.DefaultConfig(),
	}
}

// Config is the configuration of an oteldb log safety stanza operator.
type Config struct {
	helper.TransformerConfig `mapstructure:",squash"`
	safetyconfig.Config      `mapstructure:",squash"`
}

// Build builds an oteldb log safety stanza operator.
func (c Config) Build(set component.TelemetrySettings) (operator.Operator, error) {
	if err := c.Config.Validate(); err != nil {
		return nil, err
	}

	transformerOperator, err := c.TransformerConfig.Build(set)
	if err != nil {
		return nil, err
	}

	return newTransformer(transformerOperator, c), nil
}
