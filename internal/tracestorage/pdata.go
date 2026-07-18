package tracestorage

import (
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// SpansToTraces converts decoded spans back into an OTLP [ptrace.Traces] payload, e.g. for
// re-ingesting spans read out of storage into another backend. Spans are grouped into
// ResourceSpans/ScopeSpans by resource and scope identity, mirroring what a real OTLP
// producer would emit (consumers are expected to flatten per-span data regardless of
// grouping, so this is a cosmetic/efficiency choice, not a correctness one).
func SpansToTraces(spans []Span) ptrace.Traces {
	td := ptrace.NewTraces()

	type scopeKey struct {
		resource     otelstorage.Hash
		scopeName    string
		scopeVersion string
		scopeAttrs   otelstorage.Hash
	}
	scopes := map[scopeKey]ptrace.ScopeSpans{}

	for _, span := range spans {
		key := scopeKey{
			resource:     span.ResourceAttrs.Hash(),
			scopeName:    span.ScopeName,
			scopeVersion: span.ScopeVersion,
			scopeAttrs:   span.ScopeAttrs.Hash(),
		}
		ss, ok := scopes[key]
		if !ok {
			rs := td.ResourceSpans().AppendEmpty()
			span.ResourceAttrs.CopyTo(rs.Resource().Attributes())

			ss = rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(span.ScopeName)
			ss.Scope().SetVersion(span.ScopeVersion)
			span.ScopeAttrs.CopyTo(ss.Scope().Attributes())

			scopes[key] = ss
		}

		span.FillOTELSpan(ss.Spans().AppendEmpty())
	}

	return td
}
