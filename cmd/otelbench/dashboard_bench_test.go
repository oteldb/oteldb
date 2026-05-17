package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDashboardBenchmark_interpolate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		vars     map[string]string
		expected string
	}{
		{
			name:     "no variables",
			input:    "sum(rate(http_requests_total[5m]))",
			vars:     map[string]string{"job": "foo"},
			expected: "sum(rate(http_requests_total[5m]))",
		},
		{
			name:     "one variable",
			input:    `sum(rate(http_requests_total{job="$job"}[5m]))`,
			vars:     map[string]string{"job": "api-server"},
			expected: `sum(rate(http_requests_total{job="api-server"}[5m]))`,
		},
		{
			name:  "multiple variables",
			input: `sum(rate(http_requests_total{job="$job", instance="$instance"}[$__rate_interval]))`,
			vars: map[string]string{
				"job":             "api-server",
				"instance":        "localhost:8080",
				"__rate_interval": "1m",
			},
			expected: `sum(rate(http_requests_total{job="api-server", instance="localhost:8080"}[1m]))`,
		},
		{
			name:     "missing variable",
			input:    `sum(rate(http_requests_total{job="$missing"}[5m]))`,
			vars:     map[string]string{"job": "api-server"},
			expected: `sum(rate(http_requests_total{job=""}[5m]))`,
		},
		{
			name:     "curly braces",
			input:    `sum(rate(http_requests_total{job="${job}"}[5m]))`,
			vars:     map[string]string{"job": "api-server"},
			expected: `sum(rate(http_requests_total{job="api-server"}[5m]))`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &DashboardBenchmark{
				vars: tt.vars,
			}
			result := b.interpolate(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
