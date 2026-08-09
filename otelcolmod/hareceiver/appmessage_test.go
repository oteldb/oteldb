package hareceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The accepted shapes are pinned by the logs_*.json golden files; this covers
// what must not be mistaken for an application log line.
func TestParseAppMessageRejects(t *testing.T) {
	tests := []struct {
		name string
		msg  string
	}{
		{name: "Empty", msg: ""},
		{name: "Systemd", msg: "Started libcontainer container 0000."},
		{name: "Logfmt", msg: `time="2026-08-09T13:46:50+03:00" level=info msg="connecting to shim"`},
		{name: "NoTimestamp", msg: "INFO (MainThread) [a.b] hi"},
		{name: "LowerCaseLevel", msg: "2026-08-09 15:00:05.950 info (MainThread) [a.b] hi"},
		{name: "NoLogger", msg: "2026-08-09 15:00:05.950 INFO (MainThread) hi"},
		{name: "NoThread", msg: "2026-08-09 15:00:05.950 INFO [a.b] hi"},
		{name: "NewlineInThread", msg: "2026-08-09 15:00:05.950 INFO (Main\nThread) [a.b] hi"},
		{
			name: "SecondLineOnly",
			msg:  "plain\n2026-08-09 15:00:05.950 INFO (MainThread) [a.b] hi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := parseAppMessage(tt.msg)
			require.False(t, ok)
		})
	}
}

func FuzzParseAppMessage(f *testing.F) {
	for file := range readTestData(f) {
		for _, e := range ParseEntries(file.Body) {
			f.Add(e.Message)
		}
	}

	f.Fuzz(func(t *testing.T, msg string) {
		app, ok := parseAppMessage(msg)
		if !ok {
			return
		}
		// The parts must come out of the original, or the body would silently
		// gain content that was never logged.
		require.NotEmpty(t, app.Level)
		require.NotContains(t, app.Level, "\n")
		require.NotContains(t, app.Thread, "\n")
		require.NotContains(t, app.Logger, "\n")
		require.LessOrEqual(t, len(app.Message), len(msg))
	})
}
