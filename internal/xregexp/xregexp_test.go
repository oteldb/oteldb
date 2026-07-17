package xregexp

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiterals(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{``, nil},
		{`foo`, []string{"foo"}},
		{`foo.*bar`, []string{"foo", "bar"}},
		{`foo bar baz`, []string{"foo bar baz"}},
		{`error.*timeout`, []string{"error", "timeout"}},
		{`(foo)(bar)`, []string{"foo", "bar"}}, // capturing groups
		{`^user logged in$`, []string{"user logged in"}},
		{`foo|bar`, nil},                         // alternation → nothing required
		{`.*`, nil},                              // no literal
		{`[0-9]+`, nil},                          // char class
		{`(?i)foo`, nil},                         // case-insensitive → skipped
		{`foo(bar)?baz`, []string{"foo", "baz"}}, // optional middle, edges required
		{`(`, nil},                               // invalid regexp → nil
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, Literals(tt.pattern))
		})
	}
}

// FuzzLiterals asserts the required-literal invariant: any string matching the pattern contains
// every extracted literal as a substring — otherwise a prefilter built from it would prune a
// genuine match.
func FuzzLiterals(f *testing.F) {
	for _, p := range []string{`foo.*bar`, `error.*timeout`, `foo(bar)?baz`, `^user logged in$`} {
		f.Add(p, "xfoobarx")
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		re, err := regexp.Compile(pattern)
		if err != nil || !re.MatchString(value) {
			return // only matching values constrain the invariant
		}
		for _, lit := range Literals(pattern) {
			require.Containsf(t, value, lit,
				"value %q matches %q but lacks required literal %q", value, pattern, lit)
		}
	})
}
