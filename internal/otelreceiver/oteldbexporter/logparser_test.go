package oteldbexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestParseDetectFormats(t *testing.T) {
	lg := zaptest.NewLogger(t)

	// Must be nil, otherwise logstorage.ConsumerOptions would not install defaults.
	require.Nil(t, parseDetectFormats(nil, lg))
	require.Nil(t, parseDetectFormats([]string{}, lg))

	got := parseDetectFormats([]string{"logfmt", "unknown-format"}, lg)
	require.Len(t, got, 1)
	require.Equal(t, "logfmt", got[0].String())
}
