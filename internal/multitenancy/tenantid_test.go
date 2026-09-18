package multitenancy_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

func TestParseTenantID(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  string
	}{
		{"Simple", "acme", "acme"},
		{"Trimmed", "  acme  ", "acme"},
		{"Punctuation", "acme-prod_1.eu", "acme-prod_1.eu"},
		{"MaxLen", strings.Repeat("a", multitenancy.MaxTenantLen), strings.Repeat("a", multitenancy.MaxTenantLen)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := multitenancy.ParseTenantID(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	for _, tt := range []struct {
		name  string
		input string
	}{
		{"Empty", ""},
		{"Blank", "   "},
		{"Dot", "."},
		{"DotDot", ".."},
		{"Slash", "acme/prod"},
		{"Backslash", `acme\prod`},
		{"ShardSep", "acme/_s0"},
		{"Space", "acme prod"},
		{"NUL", "acme\x00"},
		{"Newline", "acme\nprod"},
		{"Unicode", "acmé"},
		{"TooLong", strings.Repeat("a", multitenancy.MaxTenantLen+1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := multitenancy.ParseTenantID(tt.input)
			require.Error(t, err)
		})
	}
}
