package chsql

import "regexp/syntax"

// RegexpLiterals returns the literal substrings a value must contain to match pattern, for deriving
// hasToken skip-index prefilters from a regexp line filter. pattern is parsed with the same flags
// (Perl) as regexp.Compile, so its literal structure matches the compiled matcher.
//
// Only literals that a match is *required* to contain are returned: the literal runs of a top-level
// concatenation (every part of a concat must match), descending through capturing groups. Anything
// optional or branching — alternation, repetition, character classes, `.` — yields no literal, so
// an un-prunable pattern (e.g. `foo|bar`, `.*`) returns nil and the caller falls back to a full
// match. Case-insensitive literals are skipped, since the token bloom is case-sensitive.
//
// Each returned literal is still a raw substring: callers must pass it through [SkipFirstLastToken]
// before tokenizing, because the pattern is unanchored and the literal's edge tokens may be
// fragments of a larger token in the value.
func RegexpLiterals(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	return appendRegexpLiterals(nil, re.Simplify())
}

func appendRegexpLiterals(dst []string, re *syntax.Regexp) []string {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 {
			// Case-insensitive literal: the case-sensitive token bloom cannot test it.
			return dst
		}
		return append(dst, string(re.Rune))
	case syntax.OpCapture:
		return appendRegexpLiterals(dst, re.Sub[0])
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			dst = appendRegexpLiterals(dst, sub)
		}
		return dst
	default:
		// Alternation, repetition, char class, anchors, `.` — no required literal.
		return dst
	}
}
