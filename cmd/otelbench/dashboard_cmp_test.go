package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

func TestDashboardCmp_Run(t *testing.T) {
	color.NoColor = true
	tests := []struct {
		name     string
		base     string
		curr     string
		markdown bool
	}{
		{
			name: "basic",
			base: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 100ms
  p99: 150ms
- panel: Panel 2
  query: up
  avg: 10ms
  p99: 12ms
`,
			curr: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 120ms
  p99: 180ms
- panel: Panel 2
  query: up
  avg: 8ms
  p99: 10ms
`,
		},
		{
			name: "markdown",
			base: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 100ms
  p99: 150ms
- panel: Panel 2
  query: up
  avg: 10ms
  p99: 12ms
`,
			curr: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 120ms
  p99: 180ms
- panel: Panel 2
  query: up
  avg: 8ms
  p99: 10ms
`,
			markdown: true,
		},
		{
			name: "new_removed",
			base: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 100ms
  p99: 150ms
- panel: Panel 2
  query: up
  avg: 10ms
  p99: 12ms
`,
			curr: `
- panel: Panel 1
  query: rate(http_requests_total[5m])
  avg: 120ms
  p99: 180ms
- panel: Panel 3
  query: histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))
  avg: 200ms
  p99: 250ms
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			basePath := filepath.Join(t.TempDir(), "base.yml")
			currPath := filepath.Join(t.TempDir(), "curr.yml")

			require.NoError(t, os.WriteFile(basePath, []byte(tt.base), 0o644))
			require.NoError(t, os.WriteFile(currPath, []byte(tt.curr), 0o644))

			var buf bytes.Buffer
			c := &DashboardCmp{
				Markdown: tt.markdown,
			}

			// Redirect output to buffer
			base, err := c.loadReport(basePath)
			require.NoError(t, err)
			curr, err := c.loadReport(currPath)
			require.NoError(t, err)

			// We need to modify Run to take an io.Writer for testing,
			// or just call the internal logic.
			// Let's refactor DashboardCmp to take an io.Writer.

			err = c.runCompare(&buf, base, curr)
			require.NoError(t, err)

			golden := filepath.Join("testdata", "dashboard_cmp", tt.name+".golden")
			if *update {
				require.NoError(t, os.WriteFile(golden, buf.Bytes(), 0o644))
			}

			expected, err := os.ReadFile(golden)
			require.NoError(t, err)

			normalize := func(s string) string {
				return strings.ReplaceAll(s, "\r\n", "\n")
			}
			require.Equal(t, normalize(string(expected)), normalize(buf.String()))
		})
	}
}
