package xregexp

import (
	"regexp/syntax"
	"slices"
	"unicode/utf8"
)

const (
	maxFiniteValues = 64
	maxFiniteBytes  = 4096
	maxFiniteDepth  = 64
)

// FiniteSet returns the exact values accepted by a whole-text anchored pattern, or nil when unsupported.
// Expansion is bounded to 64 values and 4 KiB; folding, repetition and invalid-UTF-8 matches are excluded.
func FiniteSet(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	depth := 0
	for re.Op == syntax.OpCapture {
		depth++
		if depth > maxFiniteDepth {
			return nil
		}
		re = re.Sub[0]
	}
	if re.Op != syntax.OpConcat || len(re.Sub) < 2 ||
		re.Sub[0].Op != syntax.OpBeginText || re.Sub[len(re.Sub)-1].Op != syntax.OpEndText {
		return nil
	}
	values, ok := finiteConcat(re.Sub[1:len(re.Sub)-1], depth)
	if !ok {
		return nil
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func finiteValues(re *syntax.Regexp, depth int) ([]string, bool) {
	if depth > maxFiniteDepth || re.Flags&syntax.FoldCase != 0 {
		return nil, false
	}
	switch re.Op {
	case syntax.OpEmptyMatch:
		return []string{""}, true
	case syntax.OpLiteral:
		// Regexp also matches malformed UTF-8 as RuneError; its UTF-8 spelling is not the full byte set.
		for _, r := range re.Rune {
			if !utf8.ValidRune(r) || r == utf8.RuneError {
				return nil, false
			}
		}
		value := string(re.Rune)
		return []string{value}, len(value) <= maxFiniteBytes
	case syntax.OpCharClass:
		var values []string
		for i := 0; i < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			if lo <= utf8.RuneError && utf8.RuneError <= hi || int64(hi-lo)+1 > int64(maxFiniteValues-len(values)) {
				return nil, false
			}
			for r := lo; r <= hi; r++ {
				if !utf8.ValidRune(r) {
					return nil, false
				}
				values = append(values, string(r))
			}
		}
		return values, true
	case syntax.OpCapture:
		return finiteValues(re.Sub[0], depth+1)
	case syntax.OpConcat:
		return finiteConcat(re.Sub, depth+1)
	case syntax.OpAlternate:
		var values []string
		bytes := 0
		for _, sub := range re.Sub {
			part, ok := finiteValues(sub, depth+1)
			if !ok || len(values)+len(part) > maxFiniteValues {
				return nil, false
			}
			for _, value := range part {
				bytes += len(value)
				if bytes > maxFiniteBytes {
					return nil, false
				}
			}
			values = append(values, part...)
		}
		return values, true
	default:
		return nil, false
	}
}

func finiteConcat(subs []*syntax.Regexp, depth int) ([]string, bool) {
	values := []string{""}
	for _, sub := range subs {
		part, ok := finiteValues(sub, depth+1)
		if !ok || len(values)*len(part) > maxFiniteValues {
			return nil, false
		}
		next := make([]string, 0, len(values)*len(part))
		bytes := 0
		for _, prefix := range values {
			for _, suffix := range part {
				bytes += len(prefix) + len(suffix)
				if bytes > maxFiniteBytes {
					return nil, false
				}
				next = append(next, prefix+suffix)
			}
		}
		values = next
	}
	return values, true
}
