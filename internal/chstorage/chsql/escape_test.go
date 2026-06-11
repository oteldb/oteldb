package chsql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_singleQuoted(t *testing.T) {
	tests := []struct {
		s    string
		want string
	}{
		{"", `''`},
		{"foo", `'foo'`},
		{"'\\\a\b\f\n\r\t\v", `'\'\\\a\b\f\n\r\t\v'`},
		{"\x1f", `'\x1f'`},
		{"\x7f", `'\x7f'`},
		{"!\u00a0!\u2000!\u3000!", `'!\u00a0!\u2000!\u3000!'`},
		{"\U0010ffff", `'\U0010ffff'`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, singleQuoted(tt.s))
		})
	}
}
