package logparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFmtParser(t *testing.T) {
	testParser("logfmt")(t)
}

func TestLogFmtParserDetect(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"", false},
		{`{"level":"info"}`, false},
		{`ts=2023-01-01T00:00:00Z`, false},
		{`trace_id=4bf92f3577b34da6a3ce929d0e0e4736`, false},
		{`span_id=00f067aa0ba902b7`, false},
		{`level=info`, false},
		{`msg=hi`, false},
		{`level=info msg=hi`, true},
		{`ts=2023-01-01T00:00:00Z msg=hi`, true},
		{`ts=2023-01-01T00:00:00Z level=info`, true},
		{
			"2023-12-12T15:49:36.355+0300\tDEBUG\tlogparser/zap_development_test.go:123\tIntruder alert\t" +
				`{"red_spy": "in the base", "pin": 1111, "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"}`,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			require.Equal(t, tt.want, LogFmtParser{}.Detect(tt.line))
		})
	}
}

func FuzzLogFmtParser(f *testing.F) {
	fuzzParser(f, "logfmt")
}
