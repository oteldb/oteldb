package chotel

import (
	"maps"

	"github.com/oteldb/oteldb/internal/chtrace"
)

// FilterConfig configures ClickHouse span filtering.
type FilterConfig struct {
	// Exclude drops spans whose operation_name matches any of these exact or glob patterns.
	Exclude []string `mapstructure:"exclude"`
	// Include keeps only spans matching these exact or glob patterns when non-empty.
	Include []string `mapstructure:"include"`
	// Collapse merges spans with the same operation_name within a trace.
	Collapse bool `mapstructure:"collapse"`
}

// Filter filters and optionally collapses spans.
func Filter(spans []chtrace.Trace, cfg FilterConfig) []chtrace.Trace {
	if len(spans) == 0 {
		return spans
	}
	out := make([]chtrace.Trace, 0, len(spans))
	for _, span := range spans {
		if len(cfg.Include) > 0 && !matchesAny(cfg.Include, span.OperationName) {
			continue
		}
		if matchesAny(cfg.Exclude, span.OperationName) {
			continue
		}
		out = append(out, span)
	}
	if cfg.Collapse {
		out = collapse(out)
	}
	return out
}

func matchesAny(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if pattern == value {
			return true
		}
		if globMatch(pattern, value) {
			return true
		}
	}
	return false
}

func globMatch(pattern, value string) bool {
	patternIndex := 0
	valueIndex := 0
	starIndex := -1
	matchIndex := 0

	for valueIndex < len(value) {
		if patternIndex < len(pattern) && (pattern[patternIndex] == '?' || pattern[patternIndex] == value[valueIndex]) {
			patternIndex++
			valueIndex++
			continue
		}
		if patternIndex < len(pattern) && pattern[patternIndex] == '*' {
			starIndex = patternIndex
			matchIndex = valueIndex
			patternIndex++
			continue
		}
		if starIndex != -1 {
			patternIndex = starIndex + 1
			matchIndex++
			valueIndex = matchIndex
			continue
		}
		return false
	}
	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}
	return patternIndex == len(pattern)
}

type collapseKey struct {
	traceID [16]byte
	name    string
}

func collapse(spans []chtrace.Trace) []chtrace.Trace {
	groups := make(map[collapseKey]int, len(spans))
	out := make([]chtrace.Trace, 0, len(spans))
	for _, span := range spans {
		key := collapseKey{
			traceID: span.TraceID,
			name:    span.OperationName,
		}
		idx, ok := groups[key]
		if !ok {
			groups[key] = len(out)
			out = append(out, span)
			continue
		}
		merged := &out[idx]
		if span.StartTime.Before(merged.StartTime) {
			merged.StartTime = span.StartTime
		}
		if span.FinishTime.After(merged.FinishTime) {
			merged.FinishTime = span.FinishTime
		}
		if merged.Attributes == nil {
			merged.Attributes = map[string]string{}
		}
		maps.Copy(merged.Attributes, span.Attributes)
	}
	return out
}
