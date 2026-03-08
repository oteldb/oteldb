package oteldbexporter

import (
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// sampleSpans removes spans that the sampler decides to drop,
// pruning empty ScopeSpans and ResourceSpans afterwards.
func sampleSpans(td ptrace.Traces, cfg SamplingConfig) {
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(ptrace.Span) bool { return !cfg.keep() })
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
}
