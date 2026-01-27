package logstorage

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Labels to use where prometheus compatible labels are required, e.g. loki.
const (
	LabelTraceID  = "trace_id"
	LabelSpanID   = "span_id"
	LabelSeverity = "level"
	LabelBody     = "msg"

	LabelServiceName       = "service_name"        // resource.service.name
	LabelServiceNamespace  = "service_namespace"   // resource.service.namespace
	LabelServiceInstanceID = "service_instance_id" // resource.service.instance.id
)

// Record is a log record.
type Record struct {
	Timestamp         otelstorage.Timestamp `json:"timestamp"`
	ObservedTimestamp otelstorage.Timestamp `json:"observed_timestamp"`
	TraceID           otelstorage.TraceID   `json:"trace_id"`
	SpanID            otelstorage.SpanID    `json:"span_id"`
	Flags             plog.LogRecordFlags   `json:"flags"`
	SeverityText      string                `json:"severity_text"`
	SeverityNumber    plog.SeverityNumber   `json:"severity_number"`
	Body              string                `json:"body"`
	Attrs             otelstorage.Attrs     `json:"attrs"`

	ResourceAttrs otelstorage.Attrs `json:"resource_attrs"`

	ScopeName    string            `json:"scope_name"`
	ScopeVersion string            `json:"scope_version"`
	ScopeAttrs   otelstorage.Attrs `json:"scope_attrs"`
}

// Label is a data structure for log label.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// DetectedLabel is a data structure for log label.
type DetectedLabel struct {
	Name        string `json:"name"`
	Cardinality int    `json:"cardinality"`
}

// Series defines a list of series.
type Series []map[string]string
