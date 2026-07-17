// Package xregexp extends regexp/syntax with helpers for deriving index prefilters from a pattern.
package xregexp

import "regexp/syntax"

// Literals returns the literal substrings that any value matching pattern must contain, for building
// index prefilters (e.g. a token bloom) that prune data a regexp filter cannot match. pattern is
// parsed with Perl flags, the same as regexp.Compile, so its literal structure matches the compiled
// matcher.
//
// Only literals a match is *required* to contain are returned: the literal runs of a top-level
// concatenation (every part of a concat must match), descending through capturing groups. Anything
// optional or branching — alternation, repetition, character classes, `.` — contributes no literal,
// so an un-prunable pattern (e.g. `foo|bar`, `.*`) returns nil and the caller should fall back to a
// full match. An unparsable pattern returns nil. Case-insensitive literals are skipped, since a
// case-sensitive index cannot test them.
//
// A returned literal is a raw substring, not a token: unless the pattern is anchored, the literal's
// edge tokens may be fragments of a larger token in the value, so a caller deriving whole-token
// prefilters must trim them first.
func Literals(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	return appendLiterals(nil, re.Simplify())
}

func appendLiterals(dst []string, re *syntax.Regexp) []string {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 {
			return dst
		}
		return append(dst, string(re.Rune))
	case syntax.OpCapture:
		return appendLiterals(dst, re.Sub[0])
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			dst = appendLiterals(dst, sub)
		}
		return dst
	default:
		// Alternation, repetition, char class, anchors, `.` — no required literal.
		return dst
	}
}
