package profileql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsValidLabel(t *testing.T) {
	valid := []string{
		"a",
		"_",
		"service_name",
		"service.name",
		"__name__",
		"a1.b2_c3",
	}
	for _, s := range valid {
		require.NoErrorf(t, IsValidLabel(s), "label %q should be valid", s)
		// []byte path must agree with the string path.
		require.NoErrorf(t, IsValidLabel([]byte(s)), "label %q ([]byte) should be valid", s)
	}

	invalid := []string{
		"",       // empty.
		"1abc",   // starts with a digit.
		".abc",   // starts with a dot.
		"a b",    // space.
		"a-b",    // dash.
		"a:b",    // colon.
		"служба", // non-ASCII.
	}
	for _, s := range invalid {
		require.Errorf(t, IsValidLabel(s), "label %q should be invalid", s)
		require.Errorf(t, IsValidLabel([]byte(s)), "label %q ([]byte) should be invalid", s)
	}
}

func TestLabelString(t *testing.T) {
	require.Equal(t, "service_name", Label("service_name").String())
}
