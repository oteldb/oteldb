package oteldbexporter

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestUnmarshalConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("_testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	require.Equal(t,
		&Config{DSN: "clickhouse://clickhouse:9000"},
		cfg,
	)
}

func TestUnmarshalLogsProcessingConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("_testdata", "logs_processing.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	require.Equal(t,
		&Config{
			DSN: "clickhouse://clickhouse:9000",
			Logs: LogsConfig{
				Processing: LogsProcessingConfig{
					TriggerAttributes: []string{
						"attributes.log.msg",
					},
					FormatAttributes: []string{
						"resource.log.format",
					},
					DetectFormats: []string{
						"generic-json",
					},
				},
			},
		},
		cfg,
	)
}
