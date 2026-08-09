package hareceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestDetectSeverity(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		wantSev plog.SeverityNumber
		wantTxt string
	}{
		{
			name:    "CoreFormat",
			msg:     "2026-08-09 12:00:00.099 WARNING (MainThread) [homeassistant.core] slow",
			wantSev: plog.SeverityNumberWarn,
			wantTxt: "WARNING",
		},
		{
			name:    "SupervisorFormat",
			msg:     "26-08-09 12:00:00 ERROR (MainThread) [supervisor.jobs] failed",
			wantSev: plog.SeverityNumberError,
			wantTxt: "ERROR",
		},
		{
			name:    "LeadingLevel",
			msg:     "CRITICAL boom",
			wantSev: plog.SeverityNumberFatal,
			wantTxt: "CRITICAL",
		},
		{name: "TooLate", msg: "a b c d ERROR nope"},
		{name: "LowerCase", msg: "error: something"},
		{name: "Substring", msg: "ERRORS happened"},
		{name: "NoLevel", msg: "just a message"},
		{name: "Empty", msg: ""},
		{name: "OnlySpaces", msg: "   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sev, text, ok := detectSeverity(tt.msg)
			require.Equal(t, tt.wantTxt != "", ok)
			require.Equal(t, tt.wantSev, sev)
			require.Equal(t, tt.wantTxt, text)
		})
	}
}
