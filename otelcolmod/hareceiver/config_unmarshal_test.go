package hareceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalCustomFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":              "https://homeassistant.example:8123",
		"token":                 "configured-token",
		"sources":               []any{map[string]any{"kind": "core"}},
		"poll_interval":         "42s",
		"batch_size":            17,
		"parse_message":         false,
		"recombine_window":      "2s",
		"severity_from_message": true,
		"storage":               "file_storage/cursors",
		"keepalive":             map[string]any{"idle_conn_timeout": "43s"},
	})
	require.NoError(t, conf.Unmarshal(cfg))
	require.Equal(t, configopaque.String("configured-token"), cfg.Token)
	require.Equal(t, []Source{{Kind: SourceKindCore}}, cfg.Sources)
	require.Equal(t, 42*time.Second, cfg.PollInterval)
	require.Equal(t, 17, cfg.BatchSize)
	require.False(t, cfg.ParseMessage)
	require.Equal(t, 2*time.Second, cfg.RecombineWindow)
	require.True(t, cfg.SeverityFromMessage)
	require.Equal(t, component.NewIDWithName(component.MustNewType("file_storage"), "cursors"), *cfg.StorageID)
	require.NotNil(t, cfg.Keepalive.Get())
	require.Equal(t, 43*time.Second, cfg.Keepalive.Get().IdleConnTimeout)
	require.NoError(t, cfg.Validate())
}

func TestConfigUnmarshalHTTPValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields map[string]any
	}{
		{name: "mixed_keepalive", fields: map[string]any{"idle_conn_timeout": "1m", "keepalive": map[string]any{"idle_conn_timeout": "2m"}}},
		{name: "mixed_keepalive_enabled", fields: map[string]any{"disable_keep_alives": true, "keepalive": map[string]any{"enabled": true}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, confmap.NewFromStringMap(tt.fields).Unmarshal(createDefaultConfig()))
		})
	}
}
