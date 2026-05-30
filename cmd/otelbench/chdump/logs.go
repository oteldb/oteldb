package chdump

import (
	"iter"
	"slices"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Logs is a parsed dump of logs.
type Logs struct {
	ServiceInstanceID *proto.ColLowCardinality[string]
	ServiceName       *proto.ColLowCardinality[string]
	ServiceNamespace  *proto.ColLowCardinality[string]

	Timestamp *proto.ColDateTime64

	SeverityNumber *proto.ColUInt8
	SeverityText   *proto.ColLowCardinality[string]

	TraceFlags *proto.ColUInt8
	TraceID    *proto.ColRawOf[otelstorage.TraceID]
	SpanID     *proto.ColRawOf[otelstorage.SpanID]

	Body *proto.ColStr

	Attributes *Attributes
	Resource   *Attributes

	Scope        *Attributes
	ScopeName    *proto.ColLowCardinality[string]
	ScopeVersion *proto.ColLowCardinality[string]
}

// NewLogs creates a new [Logs].
func NewLogs() *Logs {
	c := &Logs{
		ServiceInstanceID: new(proto.ColStr).LowCardinality(),
		ServiceName:       new(proto.ColStr).LowCardinality(),
		ServiceNamespace:  new(proto.ColStr).LowCardinality(),

		Timestamp: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),

		SeverityText:   new(proto.ColStr).LowCardinality(),
		SeverityNumber: new(proto.ColUInt8),

		TraceFlags: new(proto.ColUInt8),
		TraceID:    new(proto.ColRawOf[otelstorage.TraceID]),
		SpanID:     new(proto.ColRawOf[otelstorage.SpanID]),

		Body: new(proto.ColStr),

		Attributes: NewAttributes(),
		Resource:   NewAttributes(),

		Scope:        NewAttributes(),
		ScopeName:    new(proto.ColStr).LowCardinality(),
		ScopeVersion: new(proto.ColStr).LowCardinality(),
	}

	return c
}

// Result returns a [Logs] result.
func (c *Logs) Result() proto.Results {
	return slices.Collect(c.columns())
}

// Reset resets [Logs] data.
func (c *Logs) Reset() {
	for col := range c.columns() {
		col.Data.Reset()
	}
}

func (c *Logs) columns() iter.Seq[proto.ResultColumn] {
	return func(yield func(proto.ResultColumn) bool) {
		for _, col := range []proto.ResultColumn{
			{Name: "service_instance_id", Data: c.ServiceInstanceID},
			{Name: "service_name", Data: c.ServiceName},
			{Name: "service_namespace", Data: c.ServiceNamespace},

			{Name: "timestamp", Data: c.Timestamp},

			{Name: "severity_text", Data: c.SeverityText},
			{Name: "severity_number", Data: c.SeverityNumber},

			{Name: "trace_id", Data: c.TraceID},
			{Name: "span_id", Data: c.SpanID},
			{Name: "trace_flags", Data: c.TraceFlags},

			{Name: "body", Data: c.Body},

			{Name: "attribute", Data: c.Attributes},
			{Name: "resource", Data: c.Resource},

			{Name: "scope", Data: c.Scope},
			{Name: "scope_name", Data: c.ScopeName},
			{Name: "scope_version", Data: c.ScopeVersion},
		} {
			if !yield(col) {
				return
			}
		}
	}
}
