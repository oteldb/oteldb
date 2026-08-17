package config_test

import (
	"encoding/json"
	"testing"

	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// TestListenerShape pins that [config.Listener] is spelled inline by every block embedding it. It
// fails if an embedding loses its `,inline` tag, which would nest bind/auth under a "listener" key
// in YAML and silently break every existing config file.
func TestListenerShape(t *testing.T) {
	t.Parallel()

	auth := []config.Auth{{
		Type:   config.AuthTypeBearerToken,
		Tokens: []httpmiddleware.Token{{Token: "secret"}},
	}}

	blocks := map[string]any{
		"Prometheus":  &config.Prometheus{Listener: config.Listener{Bind: ":9090", Auth: auth}},
		"Loki":        &config.Loki{Listener: config.Listener{Bind: ":3100", Auth: auth}},
		"Tempo":       &config.Tempo{Listener: config.Listener{Bind: ":3200", Auth: auth}},
		"Pyroscope":   &config.Pyroscope{Listener: config.Listener{Bind: ":4040", Auth: auth}},
		"Admin":       &config.Admin{Listener: config.Listener{Bind: ":8090", Auth: auth}},
		"HealthCheck": &config.HealthCheck{Listener: config.Listener{Bind: ":13133", Auth: auth}},
	}

	for name, block := range blocks {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data, err := yaml.Marshal(block)
			require.NoError(t, err)

			var doc map[string]any
			require.NoError(t, yaml.Unmarshal(data, &doc))
			assert.Contains(t, doc, "bind", "bind must stay a top-level key")
			assert.Contains(t, doc, "auth", "auth must stay a top-level key")
			assert.NotContains(t, doc, "listener", "the embedded listener must be inlined")

			raw, err := json.Marshal(block)
			require.NoError(t, err)

			var jsonDoc map[string]any
			require.NoError(t, json.Unmarshal(raw, &jsonDoc))
			assert.Contains(t, jsonDoc, "bind")
			assert.Contains(t, jsonDoc, "auth")
			assert.NotContains(t, jsonDoc, "listener")
		})
	}
}

// TestListenerUnmarshal checks a config file written against the pre-embedding shape still parses.
func TestListenerUnmarshal(t *testing.T) {
	t.Parallel()

	const data = `
prometheus:
  bind: ":19090"
  max_samples: 10
  auth:
    - type: bearertoken
      tokens:
        - token: secret
loki:
  bind: ":13100"
  drilldown_enabled: true
tempo:
  bind: ":13200"
pyroscope:
  bind: ":14040"
admin:
  bind: ":18090"
health_check:
  bind: ":23133"
`

	var cfg struct {
		Prometheus  config.Prometheus  `yaml:"prometheus"`
		Loki        config.Loki        `yaml:"loki"`
		Tempo       config.Tempo       `yaml:"tempo"`
		Pyroscope   config.Pyroscope   `yaml:"pyroscope"`
		Admin       config.Admin       `yaml:"admin"`
		HealthCheck config.HealthCheck `yaml:"health_check"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(data), &cfg))

	assert.Equal(t, ":19090", cfg.Prometheus.Bind)
	assert.Equal(t, 10, cfg.Prometheus.MaxSamples)
	require.Len(t, cfg.Prometheus.Auth, 1)
	assert.Equal(t, config.AuthTypeBearerToken, cfg.Prometheus.Auth[0].Type)
	assert.Equal(t, ":13100", cfg.Loki.Bind)
	assert.True(t, cfg.Loki.DrilldownEnabled)
	assert.Equal(t, ":13200", cfg.Tempo.Bind)
	assert.Equal(t, ":14040", cfg.Pyroscope.Bind)
	assert.Equal(t, ":18090", cfg.Admin.Bind)
	assert.Equal(t, ":23133", cfg.HealthCheck.Bind)

	// The listeners are rangeable now that they share a type.
	for _, l := range []*config.Listener{
		&cfg.Prometheus.Listener,
		&cfg.Loki.Listener,
		&cfg.Tempo.Listener,
		&cfg.Pyroscope.Listener,
	} {
		assert.NotEmpty(t, l.Bind)
	}
}
