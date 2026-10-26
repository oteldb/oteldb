package prometheusremotewritereceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalCustomFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":       "localhost:19300",
		"time_threshold": "100000h",
		"keepalive":      map[string]any{"idle_timeout": "42s"},
	})
	require.NoError(t, conf.Unmarshal(cfg))
	require.Equal(t, 100000*time.Hour, cfg.TimeThreshold)
	require.Equal(t, "localhost:19300", cfg.NetAddr.Endpoint)
	require.NotNil(t, cfg.Keepalive.Get())
	require.Equal(t, 42*time.Second, cfg.Keepalive.Get().IdleTimeout)
}

func TestConfigUnmarshalHTTPValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields map[string]any
	}{
		{name: "mixed_keepalive", fields: map[string]any{"idle_timeout": "1m", "keepalive": map[string]any{"idle_timeout": "2m"}}},
		{name: "mixed_keepalive_enabled", fields: map[string]any{"keep_alives_enabled": false, "keepalive": map[string]any{"enabled": true}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, confmap.NewFromStringMap(tt.fields).Unmarshal(createDefaultConfig()))
		})
	}
}
