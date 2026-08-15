package logparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultFormats(t *testing.T) {
	want := []string{"generic-json", "klog", "zap-development", "logfmt"}
	require.Equal(t, want, DefaultFormatNames())

	formats := DefaultFormats()
	require.Len(t, formats, len(want))
	for i, p := range formats {
		require.Equal(t, want[i], p.String())
	}

	for _, name := range DefaultFormatNames() {
		p, ok := LookupFormat(name)
		require.Truef(t, ok, "format %q is not registered", name)
		require.Equal(t, name, p.String())
	}
}
