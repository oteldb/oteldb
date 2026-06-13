package chotel

import (
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/chtrace"
)

var numericAttributeMarkers = []string{
	"count",
	"thread_id",
	"_bytes",
	"_rows",
	"memory_usage",
	"thread_num",
	"exception_code",
}

var defaultClickHouseResource = resource.NewWithAttributes(
	semconv.SchemaURL,
	semconv.ServiceNameKey.String("clickhouse"),
)

// ParseAttributes parses ClickHouse span attributes into OpenTelemetry attributes.
func ParseAttributes(attrs map[string]string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
Attribute:
	for k, v := range attrs {
		for _, marker := range numericAttributeMarkers {
			if !strings.Contains(k, marker) {
				continue
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				break
			}
			out = append(out, attribute.Int64(k, n))
			continue Attribute
		}
		out = append(out, attribute.String(k, v))
	}
	return out
}

// ConvertTrace converts ClickHouse trace into OpenTelemetry SDK span.
func ConvertTrace(t chtrace.Trace) sdktrace.ReadOnlySpan {
	return tracetest.SpanStub{
		SpanKind:   t.Kind,
		Resource:   defaultClickHouseResource,
		Name:       t.OperationName,
		StartTime:  t.StartTime,
		EndTime:    t.FinishTime,
		Attributes: ParseAttributes(t.Attributes),
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: t.TraceID,
			SpanID:  t.SpanID,
		}),
		Parent: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: t.TraceID,
			SpanID:  t.ParentSpanID,
		}),
	}.Snapshot()
}

// ToTraces converts ClickHouse traces into collector pdata traces.
func ToTraces(spans []chtrace.Trace) ptrace.Traces {
	out := ptrace.NewTraces()
	if len(spans) == 0 {
		return out
	}
	rs := out.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), "clickhouse")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("chotel")
	ps := ss.Spans()
	for _, in := range spans {
		span := ps.AppendEmpty()
		span.SetTraceID(pcommon.TraceID(in.TraceID))
		span.SetSpanID(pcommon.SpanID(in.SpanID))
		if in.ParentSpanID.IsValid() {
			span.SetParentSpanID(pcommon.SpanID(in.ParentSpanID))
		}
		span.SetName(in.OperationName)
		span.SetKind(ptrace.SpanKind(in.Kind))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(in.StartTime))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(in.FinishTime))
		for _, attr := range ParseAttributes(in.Attributes) {
			value := span.Attributes().PutEmpty(string(attr.Key))
			switch attr.Value.Type() {
			case attribute.INT64:
				value.SetInt(attr.Value.AsInt64())
			default:
				value.SetStr(attr.Value.AsString())
			}
		}
	}
	return out
}
