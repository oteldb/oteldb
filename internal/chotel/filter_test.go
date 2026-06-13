package chotel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/oteldb/internal/chtrace"
)

func TestFilterIncludeExclude(t *testing.T) {
	start := time.Unix(1, 0)
	spans := testNamedSpans{
		{name: "Query/Pipeline", traceSeed: 1},
		{name: "Query/Planner", traceSeed: 1},
		{name: "TCPHandler::sendData", traceSeed: 1},
	}.traces(start)

	filtered := Filter(spans, FilterConfig{
		Include: []string{"Query/*"},
		Exclude: []string{"*Planner"},
	})

	assert.Len(t, filtered, 1)
	assert.Equal(t, "Query/Pipeline", filtered[0].OperationName)
}

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{name: "exact", pattern: "Query", value: "Query", want: true},
		{name: "star crosses slash", pattern: "*Pipeline*", value: "Query/Pipeline", want: true},
		{name: "prefix", pattern: "Query/*", value: "Query/Pipeline", want: true},
		{name: "question", pattern: "TCPHandler::sendDat?", value: "TCPHandler::sendData", want: true},
		{name: "mismatch", pattern: "Query/*", value: "TCPHandler", want: false},
		{name: "trailing star", pattern: "Query*", value: "Query", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, globMatch(tt.pattern, tt.value))
		})
	}
}

func TestFilterCollapse(t *testing.T) {
	start := time.Unix(100, 0)
	spans := testNamedSpans{
		{
			name:       "TCPHandler::sendData",
			traceSeed:  1,
			spanSeed:   10,
			startDelta: time.Millisecond,
			endDelta:   2 * time.Millisecond,
			attrs:      map[string]string{"first": "kept", "shared": "old"},
		},
		{
			name:       "TCPHandler::sendData",
			traceSeed:  1,
			spanSeed:   20,
			startDelta: 0,
			endDelta:   3 * time.Millisecond,
			attrs:      map[string]string{"second": "merged", "shared": "new"},
		},
		{
			name:       "TCPHandler::sendData",
			traceSeed:  2,
			spanSeed:   30,
			startDelta: 0,
			endDelta:   time.Millisecond,
		},
	}.traces(start)

	filtered := Filter(spans, FilterConfig{Collapse: true})

	assert.Len(t, filtered, 2)
	merged := filtered[0]
	assert.Equal(t, testSpanID(10), merged.SpanID)
	assert.Equal(t, start, merged.StartTime)
	assert.Equal(t, start.Add(3*time.Millisecond), merged.FinishTime)
	assert.Equal(t, map[string]string{
		"first":  "kept",
		"second": "merged",
		"shared": "new",
	}, merged.Attributes)
	assert.Equal(t, testTraceID(2), filtered[1].TraceID)
}

type testNamedSpan struct {
	name       string
	traceSeed  byte
	spanSeed   byte
	startDelta time.Duration
	endDelta   time.Duration
	attrs      map[string]string
}

type testNamedSpans []testNamedSpan

func (spans testNamedSpans) traces(start time.Time) []chtrace.Trace {
	out := make([]chtrace.Trace, 0, len(spans))
	for i, span := range spans {
		spanSeed := span.spanSeed
		if spanSeed == 0 {
			spanSeed = byte(i + 1)
		}
		out = append(out, testTrace(
			span.name,
			testTraceID(span.traceSeed),
			testSpanID(spanSeed),
			testSpanID(100+spanSeed),
			start.Add(span.startDelta),
			start.Add(span.endDelta),
		))
		out[len(out)-1].Attributes = span.attrs
	}
	return out
}
