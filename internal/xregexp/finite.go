package xregexp

import (
	"iter"
	"regexp/syntax"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/regexutil"
)

const (
	maxFiniteValues = 64
	maxFiniteBytes  = 4096
	maxFiniteDepth  = 64
)

// FiniteSet yields the exact values accepted by a whole-text anchored pattern, or nothing when unsupported.
// Expansion is bounded to 64 values and 4 KiB; folding, repetition and invalid-UTF-8 matches are excluded.
func FiniteSet(pattern string) iter.Seq[string] {
	return func(yield func(string) bool) {
		yieldFinite(pattern, yield)
	}
}

func yieldFinite(pattern string, yield func(string) bool) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return
	}
	depth := 0
	for re.Op == syntax.OpCapture {
		depth++
		if depth > maxFiniteDepth {
			return
		}
		re = re.Sub[0]
	}
	if re.Op != syntax.OpConcat || len(re.Sub) < 2 || re.Sub[0].Op != syntax.OpBeginText || re.Sub[len(re.Sub)-1].Op != syntax.OpEndText {
		return
	}
	body := &syntax.Regexp{Op: syntax.OpConcat, Sub: re.Sub[1 : len(re.Sub)-1]}
	if _, _, ok := finiteBounds(body, depth); !ok {
		return
	}
	for (body.Op == syntax.OpConcat && len(body.Sub) == 1) || body.Op == syntax.OpCapture {
		body = body.Sub[0]
	}
	switch body.Op {
	case syntax.OpLiteral:
		yield(string(body.Rune))
		return
	case syntax.OpEmptyMatch:
		yield("")
		return
	case syntax.OpConcat:
		if len(body.Sub) == 0 {
			yield("")
			return
		}
	}
	values := regexutil.GetOrValuesRegex(body.String())
	if len(values) > maxFiniteValues {
		return
	}
	bytes := 0
	for _, value := range values {
		bytes += len(value)
		if bytes > maxFiniteBytes {
			return
		}
	}
	for i, value := range values {
		if i > 0 && value == values[i-1] {
			continue
		}
		if !yield(value) {
			return
		}
	}
}

func finiteBounds(re *syntax.Regexp, depth int) (values, bytes int, ok bool) {
	if depth > maxFiniteDepth || re.Flags&syntax.FoldCase != 0 {
		return 0, 0, false
	}
	switch re.Op {
	case syntax.OpEmptyMatch:
		values = 1
	case syntax.OpLiteral:
		values = 1
		for _, r := range re.Rune {
			// Regexp also matches malformed UTF-8 as RuneError; its UTF-8 spelling is not the full byte set.
			if !utf8.ValidRune(r) || r == utf8.RuneError {
				return 0, 0, false
			}
			bytes += utf8.RuneLen(r)
			if bytes > maxFiniteBytes {
				return 0, 0, false
			}
		}
	case syntax.OpCharClass:
		for i := 0; i < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			if lo <= utf8.RuneError && utf8.RuneError <= hi || int64(hi-lo)+1 > int64(maxFiniteValues-values) {
				return 0, 0, false
			}
			for r := lo; r <= hi; r++ {
				if !utf8.ValidRune(r) {
					return 0, 0, false
				}
				values++
				bytes += utf8.RuneLen(r)
			}
		}
	case syntax.OpCapture:
		return finiteBounds(re.Sub[0], depth+1)
	case syntax.OpConcat, syntax.OpAlternate:
		if re.Op == syntax.OpConcat {
			values = 1
		}
		for _, sub := range re.Sub {
			n, size, valid := finiteBounds(sub, depth+1)
			if !valid {
				return 0, 0, false
			}
			if re.Op == syntax.OpConcat {
				bytes, values = bytes*n+size*values, values*n
			} else {
				bytes, values = bytes+size, values+n
			}
			if values > maxFiniteValues || bytes > maxFiniteBytes {
				return 0, 0, false
			}
		}
	default:
		return 0, 0, false
	}
	return values, bytes, values <= maxFiniteValues && bytes <= maxFiniteBytes
}
