package chdump

import (
	"iter"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Spans is a parsed dump of traces_spans.
type Spans struct {
	ServiceInstanceID *proto.ColLowCardinality[string]
	ServiceName       *proto.ColLowCardinality[string]
	ServiceNamespace  *proto.ColLowCardinality[string]

	TraceID       *proto.ColRawOf[otelstorage.TraceID]
	SpanID        *proto.ColRawOf[otelstorage.SpanID]
	TraceState    *proto.ColStr
	ParentSpanID  *proto.ColRawOf[otelstorage.SpanID]
	Name          *proto.ColLowCardinality[string]
	Kind          *proto.ColEnum8
	Start         *proto.ColDateTime64
	End           *proto.ColDateTime64
	StatusCode    *proto.ColUInt8
	StatusMessage *proto.ColLowCardinality[string]
	BatchID       *proto.ColUUID

	Attributes *Attributes
	Resource   *Attributes

	Scope        *Attributes
	ScopeName    *proto.ColLowCardinality[string]
	ScopeVersion *proto.ColLowCardinality[string]

	EventsNames      *proto.ColArr[string]
	EventsTimestamps *proto.ColArr[time.Time]
	EventsAttributes *proto.ColArr[[]byte]

	LinksTraceIDs    *proto.ColArr[otelstorage.TraceID]
	LinksSpanIDs     *proto.ColArr[otelstorage.SpanID]
	LinksTracestates *proto.ColArr[string]
	LinksAttributes  *proto.ColArr[[]byte]
}

// NewSpans creates a new [Spans].
func NewSpans() *Spans {
	c := &Spans{
		ServiceInstanceID: new(proto.ColStr).LowCardinality(),
		ServiceName:       new(proto.ColStr).LowCardinality(),
		ServiceNamespace:  new(proto.ColStr).LowCardinality(),

		TraceID:       new(proto.ColRawOf[otelstorage.TraceID]),
		SpanID:        new(proto.ColRawOf[otelstorage.SpanID]),
		TraceState:    new(proto.ColStr),
		ParentSpanID:  new(proto.ColRawOf[otelstorage.SpanID]),
		Name:          new(proto.ColStr).LowCardinality(),
		Kind:          new(proto.ColEnum8),
		Start:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		End:           new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		StatusCode:    new(proto.ColUInt8),
		StatusMessage: new(proto.ColStr).LowCardinality(),
		BatchID:       new(proto.ColUUID),

		Attributes: NewAttributes(),
		Resource:   NewAttributes(),

		Scope:        NewAttributes(),
		ScopeName:    new(proto.ColStr).LowCardinality(),
		ScopeVersion: new(proto.ColStr).LowCardinality(),

		EventsNames:      new(proto.ColStr).Array(),
		EventsTimestamps: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array(),
		EventsAttributes: new(proto.ColBytes).Array(),

		LinksTraceIDs:    proto.NewArray(&proto.ColRawOf[otelstorage.TraceID]{}),
		LinksSpanIDs:     proto.NewArray(&proto.ColRawOf[otelstorage.SpanID]{}),
		LinksTracestates: new(proto.ColStr).Array(),
		LinksAttributes:  new(proto.ColBytes).Array(),
	}

	return c
}

// Result returns a [Spans] result.
func (c *Spans) Result() proto.Results {
	return slices.Collect(c.columns())
}

// Reset resets [Spans] data.
func (c *Spans) Reset() {
	for col := range c.columns() {
		col.Data.Reset()
	}
}

func (c *Spans) columns() iter.Seq[proto.ResultColumn] {
	return func(yield func(proto.ResultColumn) bool) {
		for _, col := range []proto.ResultColumn{
			{Name: "service_instance_id", Data: c.ServiceInstanceID},
			{Name: "service_name", Data: c.ServiceName},
			{Name: "service_namespace", Data: c.ServiceNamespace},

			{Name: "trace_id", Data: c.TraceID},
			{Name: "span_id", Data: c.SpanID},
			{Name: "trace_state", Data: c.TraceState},
			{Name: "parent_span_id", Data: c.ParentSpanID},
			{Name: "name", Data: c.Name},
			{Name: "kind", Data: c.Kind},
			{Name: "start", Data: c.Start},
			{Name: "end", Data: c.End},
			{Name: "status_code", Data: c.StatusCode},
			{Name: "status_message", Data: c.StatusMessage},
			{Name: "batch_id", Data: c.BatchID},

			{Name: "attribute", Data: c.Attributes},
			{Name: "resource", Data: c.Resource},

			{Name: "scope", Data: c.Scope},
			{Name: "scope_name", Data: c.ScopeName},
			{Name: "scope_version", Data: c.ScopeVersion},

			{Name: "events_timestamps", Data: c.EventsTimestamps},
			{Name: "events_names", Data: c.EventsNames},
			{Name: "events_attributes", Data: c.EventsAttributes},

			{Name: "links_trace_ids", Data: c.LinksTraceIDs},
			{Name: "links_span_ids", Data: c.LinksSpanIDs},
			{Name: "links_tracestates", Data: c.LinksTracestates},
			{Name: "links_attributes", Data: c.LinksAttributes},
		} {
			if !yield(col) {
				return
			}
		}
	}
}
