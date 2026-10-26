package xregexp

import (
	"iter"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// Matcher prepares exact finite sets and literal substring searches, retaining regexp fallback.
type Matcher struct {
	re         *regexp.Regexp
	values     []string
	set        map[string]struct{}
	literal    string
	contains   bool
	substrings []string
}

// NewMatcher preserves the matching semantics of re, including its anchors and flags.
func NewMatcher(re *regexp.Regexp) *Matcher {
	m := &Matcher{re: re}
	if re == nil {
		return m
	}
	if literal, complete := re.LiteralPrefix(); complete && !strings.ContainsRune(literal, utf8.RuneError) {
		m.literal, m.contains = literal, true
		return m
	}
	m.values = slices.Collect(FiniteSet(re.String()))
	if len(m.values) == 0 {
		values := slices.Collect(FiniteSet("^(?:" + re.String() + ")$"))
		if len(values) <= 4 {
			m.substrings = values
		}
	}
	if len(m.values) > 8 {
		m.set = make(map[string]struct{}, len(m.values))
		for _, value := range m.values {
			m.set[value] = struct{}{}
		}
	}
	return m
}

// MatchString reports whether the value matches the original regexp.
func (m *Matcher) MatchString(value string) bool {
	switch {
	case m.contains:
		return strings.Contains(value, m.literal)
	case len(m.substrings) > 0:
		for _, literal := range m.substrings {
			if strings.Contains(value, literal) {
				return true
			}
		}
		return false
	case m.set != nil:
		_, ok := m.set[value]
		return ok
	case len(m.values) > 0:
		return slices.Contains(m.values, value)
	default:
		return m.re != nil && m.re.MatchString(value)
	}
}

// Values yields an exact finite value set, or nothing when matching is not finite.
func (m *Matcher) Values() iter.Seq[string] { return slices.Values(m.values) }
