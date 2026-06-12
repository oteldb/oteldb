package chotel

import (
	"maps"
	"regexp"
	"strings"

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
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, `\*`, ".*")
	pattern = strings.ReplaceAll(pattern, `\?`, ".")
	matched, err := regexp.MatchString("^"+pattern+"$", value)
	return err == nil && matched
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
