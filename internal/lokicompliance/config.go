package lokicompliance

import (
	"bytes"
	"os"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

// Config models the main configuration file.
type Config struct {
	ReferenceTargetConfig TargetConfig       `yaml:"reference_target_config"`
	TestTargetConfig      TargetConfig       `yaml:"test_target_config"`
	TestCases             []*TestCasePattern `yaml:"test_cases"`
	QueryParameters       QueryParameters    `yaml:"query_parameters"`
}

// TestCase represents a given query (pattern) to be tested.
type TestCasePattern struct {
	Query          string `yaml:"query"`
	SkipComparison bool   `yaml:"skip_comparison,omitempty"`
	ShouldFail     bool   `yaml:"should_fail,omitempty"`
	ShouldBeEmpty  bool   `yaml:"should_be_empty,omitempty"`
}

type QueryParameters struct {
	EndTime        string            `yaml:"end_time"`
	RangeInSeconds float64           `yaml:"range_in_seconds"`
	StepInSeconds  float64           `yaml:"step_in_seconds"`
	Direction      lokiapi.Direction `yaml:"direction"`
	Limit          *int              `yaml:"limit"`
}

// TargetConfig represents the configuration of a single Prometheus API endpoint.
type TargetConfig struct {
	// ReadyQuery is a log query to check instance readiness.
	ReadyQuery string `yaml:"ready_query"`
	QueryURL   string `yaml:"query_url"`
	PushURL    string `yaml:"push_url,omitempty"`
}

// LoadFromFiles parses the given YAML files into a Config.
func LoadFromFiles(filenames []string) (*Config, error) {
	var buf bytes.Buffer
	for _, f := range filenames {
		content, err := os.ReadFile(f) // #nosec G304
		if err != nil {
			return nil, errors.Wrapf(err, "reading config file %s", f)
		}
		if _, err := buf.Write(content); err != nil {
			return nil, errors.Wrapf(err, "appending config file %s to buffer", f)
		}
	}
	cfg, err := Load(buf.Bytes())
	if err != nil {
		return nil, errors.Wrapf(err, "parsing YAML files %s", filenames)
	}
	return cfg, nil
}

// Load parses the YAML input into a Config.
func Load(content []byte) (*Config, error) {
	cfg := &Config{}

	dec := yaml.NewDecoder(bytes.NewReader(content))
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
