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

func TestUnmarshalMetricsExemplarsDropConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("_testdata", "metrics_exemplars_drop.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	require.Equal(t,
		&Config{
			DSN: "clickhouse://clickhouse:9000",
			Metrics: MetricsConfig{
				Exemplars: SamplingConfig{
					Drop: true,
				},
			},
		},
		cfg,
	)
}

func TestUnmarshalTracesSpansSampleConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("_testdata", "traces_spans_sample.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	require.Equal(t,
		&Config{
			DSN: "clickhouse://clickhouse:9000",
			Traces: TracesConfig{
				Spans: SamplingConfig{Rate: 0.1},
			},
		},
		cfg,
	)
}

func TestUnmarshalLogsRecordsDropConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("_testdata", "logs_records_drop.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, cm.Unmarshal(&cfg))
	require.Equal(t,
		&Config{
			DSN: "clickhouse://clickhouse:9000",
			Logs: LogsConfig{
				Records: SamplingConfig{Drop: true},
			},
		},
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
