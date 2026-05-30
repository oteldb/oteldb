package tracestorage

import (
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
)

// Span is a data structure for span.
type Span struct {
	TraceID       otelstorage.TraceID   `json:"trace_id"`
	SpanID        otelstorage.SpanID    `json:"span_id"`
	TraceState    string                `json:"trace_state"`
	ParentSpanID  otelstorage.SpanID    `json:"parent_span_id"`
	Name          string                `json:"name"`
	Kind          int32                 `json:"kind"`
	Start         otelstorage.Timestamp `json:"start"`
	End           otelstorage.Timestamp `json:"end"`
	Attrs         otelstorage.Attrs     `json:"attrs"`
	StatusCode    int32                 `json:"status_code"`
	StatusMessage string                `json:"status_message"`

	BatchID       uuid.UUID         `json:"batch_id"`
	ResourceAttrs otelstorage.Attrs `json:"resource_attrs"`

	ScopeName    string            `json:"scope_name"`
	ScopeVersion string            `json:"scope_version"`
	ScopeAttrs   otelstorage.Attrs `json:"scope_attrs"`

	Events []Event `json:"events"`
	Links  []Link  `json:"links"`
}

// ServiceName gets service name from attributes.
func (span Span) ServiceName() (string, bool) {
	res := span.ResourceAttrs
	if res.IsZero() {
		return "", false
	}
	attr, ok := res.AsMap().Get("service.name")
	if !ok {
		return "", false
	}
	return attr.AsString(), true
}

// Event is a Span event.
type Event struct {
	Timestamp otelstorage.Timestamp `json:"timestamp"`
	Name      string                `json:"name"`
	Attrs     otelstorage.Attrs     `json:"attrs"`
}

// Link is a Span link.
type Link struct {
	TraceID    otelstorage.TraceID `json:"trace_id"`
	SpanID     otelstorage.SpanID  `json:"span_id"`
	TraceState string              `json:"trace_state"`
	Attrs      otelstorage.Attrs   `json:"attrs"`
}

// Tag is a data structure for tag.
type Tag struct {
	Name  string                 `json:"name"`
	Value string                 `json:"value"`
	Type  traceql.StaticType     `json:"type"`
	Scope traceql.AttributeScope `json:"scope"`
}

// TagFromAttribute creates a [Tag] from given [pcommon.Value].
func TagFromAttribute(scope traceql.AttributeScope, name string, value pcommon.Value) Tag {
	return Tag{
		name,
		value.AsString(),
		traceql.StaticTypeFromValueType(value.Type()),
		scope,
	}
}
