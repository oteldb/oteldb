package logstorage

import "github.com/go-faster/oteldb/internal/logparser"

// Labels to use where prometheus compatible labels are required, e.g. loki.
const (
	LabelTraceID  = "trace_id"
	LabelSpanID   = "span_id"
	LabelSeverity = "level"
	LabelBody     = "msg"

	LabelServiceName       = "service_name"        // resource.service.name
	LabelServiceNamespace  = "service_namespace"   // resource.service.namespace
	LabelServiceInstanceID = "service_instance_id" // resource.service.instance.id

	LabelDetectedLevel = "detected_level" // used by Loki/Grafana in many cases
)

// Record is a log record.
type Record = logparser.Record

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
