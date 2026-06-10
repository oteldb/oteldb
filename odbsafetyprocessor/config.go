// Package odbsafetyprocessor provides the oteldb agent log safety processor.
package odbsafetyprocessor

import (
	"go.opentelemetry.io/collector/component"

	"github.com/oteldb/oteldb/internal/odbsafety"
)

const (
	typeStr = "odbsafety"
)

var typ = component.MustNewType(typeStr)

// Config defines the odbsafety processor configuration.
type Config struct {
	odbsafety.Config `mapstructure:",squash"`

	Workload  string `mapstructure:"workload"`
	Namespace string `mapstructure:"namespace"`
}

func createDefaultConfig() component.Config {
	return &Config{
		Config: odbsafety.DefaultConfig(),
	}
}
