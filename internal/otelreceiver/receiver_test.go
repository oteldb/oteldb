package otelreceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/otelcol"
)

func TestExporterFactoryMap(t *testing.T) {
	factories, err := exporterFactoryMap()
	require.NoError(t, err)

	got := make([]string, 0, len(factories))
	for typ := range factories {
		got = append(got, typ.String())
	}
	for _, want := range []string{
		"oteldbexporter",
		"otlp",
		"otlphttp",
		"debug",
		"nop",
	} {
		require.Contains(t, got, want)
	}
}

func TestCollectorConfigWithOTLPExporter(t *testing.T) {
	cfg := map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": "localhost:0",
					},
				},
			},
		},
		"exporters": map[string]any{
			"otlp": map[string]any{
				"endpoint": "localhost:4317",
			},
			"otlphttp": map[string]any{
				"endpoint": "http://localhost:4318",
			},
			"debug": map[string]any{},
			"nop":   map[string]any{},
		},
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs": map[string]any{
					"receivers": []string{"otlp"},
					"exporters": []string{"otlp", "otlphttp", "debug", "nop"},
				},
			},
		},
	}

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		Factories: Factories(TelemetrySettings{}),
		BuildInfo: component.NewDefaultBuildInfo(),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{"oteldb:/"},
				ProviderFactories: []confmap.ProviderFactory{
					confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
						return NewMapProvider("oteldb", cfg)
					}),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, col.DryRun(t.Context()))
}
