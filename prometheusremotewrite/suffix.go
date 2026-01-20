package prometheusremotewrite

import "strings"

func metricSuffixes(input string) (s1, s2 string) {
	switch strings.Count(input, "_") {
	case 0, 1:
		return s1, s2
	default:
		input, s2 = lastCutByte(input, '_')
		_, s1 = lastCutByte(input, '_')
	}
	return s1, s2
}

func lastCutByte(s string, sep byte) (before, after string) {
	if i := strings.LastIndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

// IsValidSuffix checks suffix of histogram series.
func IsValidSuffix(suffix string) bool {
	switch suffix {
	case "max", "sum", "count", "total":
		return true
	}
	return false
}

// IsValidCumulativeSuffix checks suffix of summary series.
func IsValidCumulativeSuffix(suffix string) bool {
	switch suffix {
	case "sum", "count", "total":
		return true
	}
	return false
}

// IsValidUnit checks unit suffix.
func IsValidUnit(unit string) bool {
	switch unit {
	case "seconds", "bytes":
		return true
	}
	return false
}
