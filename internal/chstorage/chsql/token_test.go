package chsql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSingleToken(t *testing.T) {
	tests := []struct {
		s    string
		want bool
	}{
		{``, false},
		{`10`, true},
		{`abc`, true},
		{`помидоры`, true},
		{`abc 10`, false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, IsSingleToken(tt.s))
		})
	}
}

func TestCollectTokens(t *testing.T) {
	tests := []struct {
		s    string
		want []string
	}{
		{``, nil},
		{` `, nil},
		{`10`, []string{"10"}},
		{` 10 `, []string{"10"}},
		{`abc`, []string{"abc"}},
		{`помидоры abc огурцы`, []string{"помидоры", "abc", "огурцы"}},
		{`"error": "ENOENT"`, []string{"error", "ENOENT"}},
		{
			`{"msg": "Request", "error": "invalid data"}`,
			[]string{"msg", "Request", "error", "invalid", "data"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var got []string
			CollectTokens(tt.s, func(tok string) bool {
				got = append(got, tok)
				return true
			})
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzCollectTokens(f *testing.F) {
	for _, s := range []string{
		`помидоры abc огурцы`,
		`{"msg": "Request", "error": "invalid data"}`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input: %#q", input)
			}
		}()
		CollectTokens(input, func(tok string) bool {
			return true
		})
	})
}
