// Package odblogparser provides an oteldb log parser stanza operator.
package odblogparser

import (
	"go.opentelemetry.io/collector/component"

	"github.com/go-faster/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"

	"github.com/oteldb/oteldb/internal/logparser"
)

// Type is the stanza operator type registered by this package.
const Type = "odblogparser"

func init() {
	RegisterDefault()
}

// NewConfig creates a new odblogparser config with default values.
func NewConfig() *Config {
	return NewConfigWithID(Type)
}

// NewConfigWithID creates a new odblogparser config with default values.
func NewConfigWithID(operatorID string) *Config {
	return &Config{
		ParserConfig:  helper.NewParserConfig(operatorID, Type),
		Detect:        true,
		DetectFormats: []string{"generic-json"},
	}
}

// Config is the configuration of an oteldb log parser stanza operator.
type Config struct {
	helper.ParserConfig `mapstructure:",squash"`

	// Format is an explicit internal/logparser format name.
	Format string `mapstructure:"format"`
	// Detect enables parser format detection when Format is not set.
	Detect bool `mapstructure:"detect"`
	// DetectFormats is the ordered list of parser format names to detect.
	DetectFormats []string `mapstructure:"detect_formats"`
}

// Build builds an oteldb log parser stanza operator.
func (c Config) Build(set component.TelemetrySettings) (operator.Operator, error) {
	parserOperator, err := c.ParserConfig.Build(set)
	if err != nil {
		return nil, err
	}

	var (
		explicit logparser.Parser
		detect   []logparser.Parser
	)
	if c.Format != "" {
		p, ok := logparser.LookupFormat(c.Format)
		if !ok {
			return nil, errors.Errorf("unknown log parser format %q", c.Format)
		}
		explicit = p
	} else if c.Detect {
		detectFormats := c.DetectFormats
		if detectFormats == nil {
			detectFormats = []string{"generic-json"}
		}
		detect = make([]logparser.Parser, 0, len(detectFormats))
		for _, format := range detectFormats {
			p, ok := logparser.LookupFormat(format)
			if !ok {
				return nil, errors.Errorf("unknown log parser detect format %q", format)
			}
			detect = append(detect, p)
		}
	}

	return &Parser{
		ParserOperator: parserOperator,
		format:         explicit,
		detectFormats:  detect,
	}, nil
}
