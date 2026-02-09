package logstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func Test_normalizeSeverity(t *testing.T) {
	tests := []struct {
		text       string
		number     plog.SeverityNumber
		wantText   string
		wantNumber plog.SeverityNumber
	}{
		{text: "", number: plog.SeverityNumberDebug, wantText: "Debug", wantNumber: plog.SeverityNumberDebug},
		{text: "debug", number: 0, wantText: "debug", wantNumber: plog.SeverityNumberDebug},
		{text: "debug", number: plog.SeverityNumberDebug, wantText: "debug", wantNumber: plog.SeverityNumberDebug},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			number, text := normalizeSeverity(tt.number, tt.text)
			assert.Equal(t, tt.wantNumber, number)
			assert.Equal(t, tt.wantText, text)
		})
	}
}

func Test_parseSeverity(t *testing.T) {
	tests := []struct {
		text string
		want plog.SeverityNumber
	}{
		{text: "", want: plog.SeverityNumberUnspecified},
		{text: "TRACE", want: plog.SeverityNumberTrace},
		{text: "trace", want: plog.SeverityNumberTrace},
		{text: "info", want: plog.SeverityNumberInfo},
		{text: "warn", want: plog.SeverityNumberWarn},
		{text: "warning", want: plog.SeverityNumberWarn},
		{text: "error", want: plog.SeverityNumberError},
		{text: "fatal", want: plog.SeverityNumberFatal},
		{text: "deBug", want: plog.SeverityNumberDebug},
		{text: " deBug ", want: plog.SeverityNumberDebug},

		{text: "my-custom-log-level", want: plog.SeverityNumberUnspecified},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, parseSeverity(tt.text))
		})
	}
}
