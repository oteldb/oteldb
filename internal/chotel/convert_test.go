package chotel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/chtrace"
)

func TestParseAttributes(t *testing.T) {
	attrs := ParseAttributes(map[string]string{
		"clickhouse.thread_id":    "42",
		"clickhouse.read_bytes":   "1024",
		"clickhouse.memory_usage": "not-a-number",
		"db.statement":            "SELECT 1",
	})

	values := make(map[string]attribute.Value, len(attrs))
	for _, attr := range attrs {
		values[string(attr.Key)] = attr.Value
	}

	assert.Equal(t, attribute.INT64, values["clickhouse.thread_id"].Type())
	assert.Equal(t, int64(42), values["clickhouse.thread_id"].AsInt64())
	assert.Equal(t, attribute.INT64, values["clickhouse.read_bytes"].Type())
	assert.Equal(t, int64(1024), values["clickhouse.read_bytes"].AsInt64())
	assert.Equal(t, attribute.STRING, values["clickhouse.memory_usage"].Type())
	assert.Equal(t, "not-a-number", values["clickhouse.memory_usage"].AsString())
	assert.Equal(t, "SELECT 1", values["db.statement"].AsString())
}

func TestConvertTrace(t *testing.T) {
	start := time.Unix(10, 20)
	end := start.Add(time.Millisecond)
	in := testTrace("operation", testTraceID(1), testSpanID(2), testSpanID(3), start, end)
	in.Attributes = map[string]string{
		"clickhouse.read_rows": "7",
		"db.statement":         "SELECT 1",
	}

	span := ConvertTrace(in)

	assert.Equal(t, in.TraceID, span.SpanContext().TraceID())
	assert.Equal(t, in.SpanID, span.SpanContext().SpanID())
	assert.Equal(t, in.TraceID, span.Parent().TraceID())
	assert.Equal(t, in.ParentSpanID, span.Parent().SpanID())
	assert.Equal(t, "operation", span.Name())
	assert.Equal(t, trace.SpanKindServer, span.SpanKind())
	assert.Equal(t, start, span.StartTime())
	assert.Equal(t, end, span.EndTime())
	assert.True(t, span.Resource().Equal(defaultClickHouseResource))

	attrs := make(map[string]attribute.Value, len(span.Attributes()))
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	assert.Equal(t, int64(7), attrs["clickhouse.read_rows"].AsInt64())
	assert.Equal(t, "SELECT 1", attrs["db.statement"].AsString())
}

func TestToTracesEmpty(t *testing.T) {
	traces := ToTraces(nil)

	assert.Equal(t, 0, traces.ResourceSpans().Len())
	assert.Equal(t, 0, traces.SpanCount())
}

func TestToTraces(t *testing.T) {
	start := time.Unix(20, 30)
	end := start.Add(2 * time.Millisecond)
	in := testTrace("Query/Pipeline", testTraceID(4), testSpanID(5), testSpanID(6), start, end)
	in.Attributes = map[string]string{
		"clickhouse.thread_num": "3",
		"db.statement":          "SELECT 1",
	}

	traces := ToTraces([]chtrace.Trace{in})

	require.Equal(t, 1, traces.ResourceSpans().Len())
	rs := traces.ResourceSpans().At(0)
	serviceName, ok := rs.Resource().Attributes().Get(string(semconv.ServiceNameKey))
	require.True(t, ok)
	assert.Equal(t, "clickhouse", serviceName.Str())

	require.Equal(t, 1, rs.ScopeSpans().Len())
	ss := rs.ScopeSpans().At(0)
	assert.Equal(t, "chotel", ss.Scope().Name())
	require.Equal(t, 1, ss.Spans().Len())
	span := ss.Spans().At(0)

	assert.Equal(t, pcommon.TraceID(in.TraceID), span.TraceID())
	assert.Equal(t, pcommon.SpanID(in.SpanID), span.SpanID())
	assert.Equal(t, pcommon.SpanID(in.ParentSpanID), span.ParentSpanID())
	assert.Equal(t, "Query/Pipeline", span.Name())
	assert.Equal(t, ptrace.SpanKindServer, span.Kind())
	assert.Equal(t, pcommon.NewTimestampFromTime(start), span.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(end), span.EndTimestamp())

	threadNum, ok := span.Attributes().Get("clickhouse.thread_num")
	require.True(t, ok)
	assert.Equal(t, int64(3), threadNum.Int())
	statement, ok := span.Attributes().Get("db.statement")
	require.True(t, ok)
	assert.Equal(t, "SELECT 1", statement.Str())
}

func testTrace(
	name string,
	traceID trace.TraceID,
	spanID trace.SpanID,
	parentSpanID trace.SpanID,
	start time.Time,
	end time.Time,
) chtrace.Trace {
	return chtrace.Trace{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		OperationName: name,
		StartTime:     start,
		FinishTime:    end,
		Attributes:    map[string]string{},
		Kind:          trace.SpanKindServer,
	}
}

func testTraceID(seed byte) trace.TraceID {
	var id trace.TraceID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testSpanID(seed byte) trace.SpanID {
	var id trace.SpanID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}
