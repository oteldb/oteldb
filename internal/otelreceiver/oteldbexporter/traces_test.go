package oteldbexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func makeTraces(resources, scopes, spans int) ptrace.Traces {
	td := ptrace.NewTraces()
	for range resources {
		rs := td.ResourceSpans().AppendEmpty()
		for range scopes {
			ss := rs.ScopeSpans().AppendEmpty()
			for range spans {
				ss.Spans().AppendEmpty()
			}
		}
	}
	return td
}

func TestSampleSpans_DropAll(t *testing.T) {
	td := makeTraces(2, 3, 4)
	require.Equal(t, 24, td.SpanCount())

	sampleSpans(td, SamplingConfig{Drop: true})

	require.Equal(t, 0, td.SpanCount())
	// Empty ResourceSpans must be pruned.
	require.Equal(t, 0, td.ResourceSpans().Len())
}

func TestSampleSpans_Disabled(t *testing.T) {
	td := makeTraces(2, 3, 4)
	sampleSpans(td, SamplingConfig{})
	require.Equal(t, 24, td.SpanCount())
	require.Equal(t, 2, td.ResourceSpans().Len())
}

func TestSampleSpans_RateOne(t *testing.T) {
	td := makeTraces(2, 3, 4)
	sampleSpans(td, SamplingConfig{Rate: 1.0})
	require.Equal(t, 24, td.SpanCount())
}

func TestSampleSpans_RateSampling(t *testing.T) {
	const n = 2000
	td := makeTraces(1, 1, n)
	sampleSpans(td, SamplingConfig{Rate: 0.5})
	got := td.SpanCount()
	// Expect ~50%, allow ±15%.
	require.InDelta(t, n/2, got, n*0.15)
}

func TestSampleSpans_PrunesEmptyScopeSpans(t *testing.T) {
	// 1 resource, 2 scopes, each with 1 span. Drop all → both scopes and resource pruned.
	td := makeTraces(1, 2, 1)
	sampleSpans(td, SamplingConfig{Drop: true})
	require.Equal(t, 0, td.ResourceSpans().Len())
}
