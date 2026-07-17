package chsql

import "slices"

// IsSingleToken whether if given string is a single token.
//
// See https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions#hastoken.
// See https://github.com/ClickHouse/ClickHouse/blob/755b73f3fc99847f40ac4d9186bb19116e709c37/src/Interpreters/ITokenExtractor.cpp#L84.
func IsSingleToken[S ~string | ~[]byte](s S) bool {
	if len(s) == 0 {
		return false
	}
	// If string does contain any non-alphanumeric ASCII characters.
	// then it is not a single token.
	return !slices.ContainsFunc([]byte(s), isTokenSeparator)
}

// CollectTokens iterates over tokens in given string.
func CollectTokens[S ~string | ~[]byte](s S, cb func(s S) bool) {
	// FIXME(tdakkota): use go1.23 iterators.
	if len(s) == 0 {
		return
	}
	// If string does contain any non-alphanumeric ASCII characters.
	// then it is not a single token.
	var (
		i, lastIdx int
		c          byte
	)
	for i, c = range []byte(s) {
		if !isTokenSeparator(c) {
			continue
		}
		tok := s[lastIdx:i]
		if len(tok) > 0 && !cb(tok) {
			return
		}
		lastIdx = i + 1
	}
	if tok := s[lastIdx:]; len(tok) > 0 {
		cb(s[lastIdx:])
	}
}

// SkipFirstLastToken drops the leading and trailing partial tokens of s: the run of token
// characters touching each end, up to the first/last token separator.
//
// It must be applied to a substring or regexp literal before deriving bloom tokens from it (via
// [CollectTokens]). A `hasToken` skip index stores whole tokens, but a literal only occurs as a
// substring of the value, so its first and last tokens may be fragments of a larger token there —
// "error" occurs inside "myerror", whose only token is "myerror" — and testing them would wrongly
// prune a granule holding a real match. Only the interior tokens, bounded by separators on both
// sides within the literal, are guaranteed whole tokens of every matching value. A single-token
// literal collapses to empty (no pruning, but no false negatives).
func SkipFirstLastToken[S ~string | ~[]byte](s S) S {
	lo, hi := 0, len(s)
	for lo < hi && !isTokenSeparator(s[lo]) {
		lo++
	}
	for hi > lo && !isTokenSeparator(s[hi-1]) {
		hi--
	}
	return s[lo:hi]
}

func isTokenSeparator(c byte) bool {
	return c < 0x80 && !isAlphaNumeric(c)
}

func isAlphaNumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}
