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

func TestSkipFirstLastToken(t *testing.T) {
	tests := []struct {
		s    string
		want string
	}{
		{``, ``},
		{`error`, ``},            // single token → nothing is a guaranteed whole token
		{`foo bar`, ` `},         // both words touch an edge → neither is safe
		{`foo bar baz`, ` bar `}, // interior word survives
		{`a.b.c`, `.b.`},         // dot separators
		{` foo `, ` foo `},       // separators already bound both edges → keep
		{`помидоры abc огурцы`, ` abc `}, // multibyte edge tokens dropped
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, SkipFirstLastToken(tt.s))
		})
	}
}

// TestSkipFirstLastTokenNoOverPrune is the regression for the substring over-prune bug: a `|= "L"`
// filter matches any body containing L as a substring, so every token derived for the hasToken
// skip index must be a whole token of such a body — otherwise a real match is pruned.
func TestSkipFirstLastTokenNoOverPrune(t *testing.T) {
	// `|= "error"` must not prune a body "myerror occurred": the naive tokenization yields "error",
	// absent from that body's tokens {"myerror","occurred"}. SkipFirstLastToken drops it.
	var got []string
	CollectTokens(SkipFirstLastToken("error"), func(tok string) bool {
		got = append(got, tok)
		return true
	})
	require.Empty(t, got, "single-word substring must yield no hasToken prefilter")
}

func FuzzSkipFirstLastToken(f *testing.F) {
	seeds := []struct{ lit, prefix, suffix string }{
		{"foo bar baz", "pre", "suf"},
		{"error", "my", " occurred"},
		{"a.b.c", "", ""},
		{"помидоры abc огурцы", "x", "y"},
	}
	for _, s := range seeds {
		f.Add(s.lit, s.prefix, s.suffix)
	}
	f.Fuzz(func(t *testing.T, lit, prefix, suffix string) {
		// value contains lit as a substring, with arbitrary surrounding bytes.
		value := prefix + lit + suffix

		valueTokens := map[string]struct{}{}
		CollectTokens(value, func(tok string) bool {
			valueTokens[tok] = struct{}{}
			return true
		})

		CollectTokens(SkipFirstLastToken(lit), func(tok string) bool {
			_, ok := valueTokens[tok]
			require.Truef(t, ok, "token %q from %q absent in Tokenize(%q) — would falsely prune", tok, lit, value)
			return true
		})
	})
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
