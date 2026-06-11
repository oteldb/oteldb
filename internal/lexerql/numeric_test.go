package lexerql

import (
	"fmt"
	"strings"
	"testing"
	"text/scanner"

	"github.com/stretchr/testify/require"
)

func TestScanUnit(t *testing.T) {
	tests := []struct {
		input   string
		want    Unit
		wantErr string
	}{
		// Bytes suffixes.
		{"10b", Unit{Type: Bytes, Text: "10b"}, ""},
		{"10KiB", Unit{Type: Bytes, Text: "10KiB"}, ""},
		{"10KB", Unit{Type: Bytes, Text: "10KB"}, ""},
		{"10MiB", Unit{Type: Bytes, Text: "10MiB"}, ""},
		{"10mb", Unit{Type: Bytes, Text: "10mb"}, ""},
		{"10MB", Unit{Type: Bytes, Text: "10MB"}, ""},
		{"10GiB", Unit{Type: Bytes, Text: "10GiB"}, ""},
		{"10GB", Unit{Type: Bytes, Text: "10GB"}, ""},
		{"10TiB", Unit{Type: Bytes, Text: "10TiB"}, ""},
		{"10TB", Unit{Type: Bytes, Text: "10TB"}, ""},
		{"10ki", Unit{Type: Bytes, Text: "10ki"}, ""},
		{"10k", Unit{Type: Bytes, Text: "10k"}, ""},
		{"10mi", Unit{Type: Bytes, Text: "10mi"}, ""},
		{"10gi", Unit{Type: Bytes, Text: "10gi"}, ""},
		{"10g", Unit{Type: Bytes, Text: "10g"}, ""},
		{"10ti", Unit{Type: Bytes, Text: "10ti"}, ""},
		{"10t", Unit{Type: Bytes, Text: "10t"}, ""},

		// Duration suffixes.
		{"10ns", Unit{Type: Duration, Text: "10ns"}, ""},
		{"10us", Unit{Type: Duration, Text: "10us"}, ""},
		{"10µs", Unit{Type: Duration, Text: "10µs"}, ""},
		{"10μs", Unit{Type: Duration, Text: "10μs"}, ""},
		{"10ms", Unit{Type: Duration, Text: "10ms"}, ""},
		{"10s", Unit{Type: Duration, Text: "10s"}, ""},
		{"10m", Unit{Type: Duration, Text: "10m"}, ""},
		{"10h", Unit{Type: Duration, Text: "10h"}, ""},
		{"10d", Unit{Type: Duration, Text: "10d"}, ""},
		{"10w", Unit{Type: Duration, Text: "10w"}, ""},
		{"1w1d1h1m1s", Unit{Type: Duration, Text: "1w1d1h1m1s"}, ""},

		// Invalid duration.
		{"1d1w", Unit{}, `not a valid duration string: "1d1w"`},

		// Invalid unit.
		{"10sms", Unit{}, `unknown unit "sms"`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var scan scanner.Scanner
			scan.Init(strings.NewReader(tt.input))

			// Read number prefix.
			scan.Scan()

			got, err := ScanUnit(&scan, scan.TokenText())
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
