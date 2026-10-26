package xregexp

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	for _, tt := range []struct {
		pattern string
		values  []string
	}{
		{`foo`, nil},
		{`^(foo|bar)$`, []string{"bar", "foo"}},
		{`^(|foo)$`, []string{"", "foo"}},
		{`^$`, []string{""}},
		{`^a[0-9]$`, []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"}},
		{`foo|bar`, nil},
		{`foo|`, nil},
		{`^foo|bar$`, nil},
		{`foo|bar|baz|qux|quux`, nil},
		{`(?i)^foo$`, nil},
		{`^foo.*bar$`, nil},
		{`\x{FFFD}`, nil},
	} {
		t.Run(tt.pattern, func(t *testing.T) {
			re := regexp.MustCompile(tt.pattern)
			m := NewMatcher(re)
			require.Equal(t, tt.values, slices.Collect(m.Values()))
			for _, value := range []string{"", "foo", "bar", "xfooy", "FOO", "a0", "a9", "a10", "foo\nbar", "\xff", "\uFFFD"} {
				require.Equal(t, re.MatchString(value), m.MatchString(value), "%q", value)
			}
		})
	}
	require.False(t, NewMatcher(nil).MatchString(""))
}

func FuzzMatcher(f *testing.F) {
	for _, seed := range [][2]string{
		{`foo`, "xfooy"},
		{`^(foo|bar)$`, "xfooy"},
		{`^(|foo)$`, ""},
		{`^a[0-9]$`, "a9"},
		{`(?i)^foo$`, "FOO"},
		{`(?s)^foo.*bar$`, "foo\nbar"},
		{`(?m)^foo$`, "before\nfoo\nafter"},
		{`\x{FFFD}`, "\xff"},
		{`\x{FFFD}`, "\uFFFD"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return
		}
		m := NewMatcher(re)
		if got, want := m.MatchString(value), re.MatchString(value); got != want {
			t.Fatalf("optimized %q over %q: got %t, want %t", pattern, value, got, want)
		}
	})
}

func BenchmarkMatcher(b *testing.B) {
	for _, pattern := range []string{`foo`, `^(foo|bar)$`, `^a[0-9]$`, `(?i)^foo$`} {
		b.Run(pattern, func(b *testing.B) {
			re := regexp.MustCompile(pattern)
			m := NewMatcher(re)
			for _, optimized := range []bool{false, true} {
				name := "regexp"
				match := re.MatchString
				if optimized {
					name, match = "prepared", m.MatchString
				}
				b.Run(name, func(b *testing.B) {
					const value = "xfooy"
					want := re.MatchString(value)
					b.ReportAllocs()
					b.SetBytes(int64(len(value)))
					for b.Loop() {
						if match(value) != want {
							b.Fatal("mismatch")
						}
					}
				})
			}
		})
	}
}

func BenchmarkSubstringAlternatives(b *testing.B) {
	for _, pattern := range []string{`foo|bar`, `foo|bar|baz|qux`} {
		b.Run(pattern, func(b *testing.B) {
			re := regexp.MustCompile(pattern)
			m := NewMatcher(re)
			for _, tc := range []struct{ name, value string }{
				{"short_match", "prefix foo suffix"},
				{"short_miss", "prefix no match suffix"},
				{"long_match", strings.Repeat("x", 4096) + "bar suffix"},
				{"long_miss", strings.Repeat("x", 4096)},
			} {
				b.Run(tc.name, func(b *testing.B) {
					for _, optimized := range []bool{false, true} {
						name, match := "regexp", re.MatchString
						if optimized {
							name, match = "prepared", m.MatchString
						}
						b.Run(name, func(b *testing.B) {
							want := re.MatchString(tc.value)
							b.ReportAllocs()
							b.SetBytes(int64(len(tc.value)))
							for b.Loop() {
								if match(tc.value) != want {
									b.Fatal("mismatch")
								}
							}
						})
					}
				})
			}
		})
	}
}
