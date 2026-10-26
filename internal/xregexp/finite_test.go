package xregexp

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFiniteSet(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, pattern string
		want          []string
	}{
		{name: "literal", pattern: `^foo$`, want: []string{"foo"}},
		{name: "alternation", pattern: `^(foo|bar)$`, want: []string{"bar", "foo"}},
		{name: "factored", pattern: `^(foo|foobar)$`, want: []string{"foo", "foobar"}},
		{name: "classes", pattern: `^[ab][12]$`, want: []string{"a1", "a2", "b1", "b2"}},
		{name: "unicode", pattern: `^(猫|犬)$`, want: []string{"犬", "猫"}},
		{name: "empty", pattern: `^$`, want: []string{""}},
		{name: "empty_alternative", pattern: `^(|foo)$`, want: []string{"", "foo"}},
		{name: "leading_empty", pattern: `^(|foo)bar$`, want: []string{"bar", "foobar"}},
		{name: "internal_empty", pattern: `^foo(bar||baz)$`, want: []string{"foo", "foobar", "foobaz"}},
		{name: "duplicate", pattern: `^(foo|foo)$`, want: []string{"foo"}},
		{name: "capture", pattern: `(^foo$)`, want: []string{"foo"}},
		{name: "explicit_anchors", pattern: `\A(?:foo|bar)\z`, want: []string{"bar", "foo"}},
		{name: "unanchored", pattern: `foo|bar`},
		{name: "partial_anchor", pattern: `^foo|bar$`},
		{name: "multiline", pattern: `(?m)^foo$`},
		{name: "folded", pattern: `(?i)^foo$`},
		{name: "unicode_fold", pattern: `^(?i:k)$`},
		{name: "substring", pattern: `^.*foo.*$`},
		{name: "repetition", pattern: `^a{2}$`},
		{name: "rune_error", pattern: "^\uFFFD$"},
		{name: "rune_error_class", pattern: `^[\x{FFFC}-\x{FFFE}]$`},
		{name: "surrogate", pattern: `^\x{D800}$`},
		{name: "surrogate_class", pattern: `^[\x{D800}-\x{D801}]$`},
		{name: "too_many", pattern: `^[a-z][a-z]$`},
		{name: "too_large", pattern: "^" + strings.Repeat("x", maxFiniteBytes+1) + "$"},
		{name: "too_deep", pattern: strings.Repeat("(", maxFiniteDepth+1) + "^foo$" + strings.Repeat(")", maxFiniteDepth+1)},
		{name: "invalid", pattern: `(`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, slices.Collect(FiniteSet(tt.pattern)))
		})
	}
}

func FuzzFiniteSet(f *testing.F) {
	for _, pattern := range []string{`^(foo|bar)$`, `^$`, `^[ab][12]$`, `(?i)^k$`, "^\uFFFD$", `foo|bar`, `^foo|bar$`} {
		for _, value := range []string{"", "foo", "xbar", "foo\n", "\xff", "\uFFFD"} {
			f.Add(pattern, value)
		}
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		values := slices.Collect(FiniteSet(pattern))
		if len(values) == 0 {
			return
		}
		re, err := regexp.Compile(pattern)
		if err != nil || re.MatchString(value) != slices.Contains(values, value) {
			t.Fatalf("finite set disagrees with regexp: %q over %q", pattern, value)
		}
		for _, member := range values {
			if !re.MatchString(member) {
				t.Fatalf("set member does not match regexp: %q over %q", pattern, member)
			}
		}
	})
}
