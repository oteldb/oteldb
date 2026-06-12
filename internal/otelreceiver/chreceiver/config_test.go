package chreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/oteldb/oteldb/internal/chotel"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	typ := component.MustNewType(typeStr)
	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewIDWithName(typ, "defaults"),
			expected: createDefaultConfig(),
		},
		{
			id: component.NewIDWithName(typ, ""),
			expected: &Config{
				DSN:      "clickhouse://default:secret@clickhouse:9000/otel",
				PollRate: time.Second,
				Filter: chotel.FilterConfig{
					Exclude:  []string{"SystemLog*"},
					Include:  []string{"Query", "*Pipeline*"},
					Collapse: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
