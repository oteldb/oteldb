package chstorage

import (
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/ddl"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/xsync"
)

var (
	spanColumnsPool      = xsync.NewPool(newSpanColumns)
	spanAttrsColumnsPool = xsync.NewPool(newSpanAttrsColumns)
)

type spanColumns struct {
	serviceInstanceID *proto.ColLowCardinality[string]
	serviceName       *proto.ColLowCardinality[string]
	serviceNamespace  *proto.ColLowCardinality[string]

	traceID       proto.ColRawOf[otelstorage.TraceID]
	spanID        proto.ColRawOf[otelstorage.SpanID]
	traceState    proto.ColStr
	parentSpanID  proto.ColRawOf[otelstorage.SpanID]
	name          *proto.ColLowCardinality[string]
	kind          proto.ColEnum8
	start         *proto.ColDateTime64
	end           *proto.ColDateTime64
	statusCode    proto.ColUInt8
	statusMessage *proto.ColLowCardinality[string]
	batchID       proto.ColUUID

	attributes *Attributes
	resource   *Attributes

	scopeName       *proto.ColLowCardinality[string]
	scopeVersion    *proto.ColLowCardinality[string]
	scopeAttributes *Attributes

	events eventsColumns
	links  linksColumns

	columns func() Columns
	Input   func() proto.Input
	Body    func(table string) string
}

func newSpanColumns() *spanColumns {
	c := &spanColumns{
		name:          new(proto.ColStr).LowCardinality(),
		start:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		end:           new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		statusMessage: new(proto.ColStr).LowCardinality(),
		events:        newEventsColumns(),
		links:         newLinksColumns(),

		serviceInstanceID: new(proto.ColStr).LowCardinality(),
		serviceName:       new(proto.ColStr).LowCardinality(),
		serviceNamespace:  new(proto.ColStr).LowCardinality(),
		attributes:        NewAttributes(colAttrs),
		resource:          NewAttributes(colResource),
		scopeName:         new(proto.ColStr).LowCardinality(),
		scopeVersion:      new(proto.ColStr).LowCardinality(),
		scopeAttributes:   NewAttributes(colScope),
	}
	c.columns = sync.OnceValue(func() Columns {
		return MergeColumns(Columns{
			{Name: "service_instance_id", Data: c.serviceInstanceID},
			{Name: "service_name", Data: c.serviceName},
			{Name: "service_namespace", Data: c.serviceNamespace},

			{Name: "trace_id", Data: &c.traceID},
			{Name: "span_id", Data: &c.spanID},
			{Name: "trace_state", Data: &c.traceState},
			{Name: "parent_span_id", Data: &c.parentSpanID},
			{Name: "name", Data: c.name},
			{Name: "kind", Data: proto.Wrap(&c.kind, kindDDL)},
			{Name: "start", Data: c.start},
			{Name: "end", Data: c.end},
			{Name: "status_code", Data: &c.statusCode},
			{Name: "status_message", Data: c.statusMessage},
			{Name: "batch_id", Data: &c.batchID},

			{Name: "scope_name", Data: c.scopeName},
			{Name: "scope_version", Data: c.scopeVersion},

			{Name: "events_timestamps", Data: c.events.timestamps},
			{Name: "events_names", Data: c.events.names},
			{Name: "events_attributes", Data: c.events.attributes},

			{Name: "links_trace_ids", Data: c.links.traceIDs},
			{Name: "links_span_ids", Data: c.links.spanIDs},
			{Name: "links_tracestates", Data: c.links.tracestates},
			{Name: "links_attributes", Data: c.links.attributes},
		},
			c.attributes.Columns(),
			c.resource.Columns(),
			c.scopeAttributes.Columns(),
		)
	})
	c.Input = sync.OnceValue(func() proto.Input {
		return c.columns().Input()
	})
	c.Body = xsync.KeyOnce(func(table string) string {
		return c.Input().Into(table)
	})
	return c
}

func (c *spanColumns) Result() proto.Results             { return c.columns().Result() }
func (c *spanColumns) ChsqlResult() []chsql.ResultColumn { return c.columns().ChsqlResult() }
func (c *spanColumns) Reset()                            { c.columns().Reset() }

func (c *spanColumns) AddRow(s tracestorage.Span) {
	c.traceID.Append(s.TraceID)
	c.spanID.Append(s.SpanID)
	c.traceState.Append(s.TraceState)
	c.parentSpanID.Append(s.ParentSpanID)
	c.name.Append(s.Name)
	c.kind.Append(proto.Enum8(s.Kind))
	c.start.Append(time.Unix(0, int64(s.Start)))
	c.end.Append(time.Unix(0, int64(s.End)))
	c.statusCode.Append(uint8(s.StatusCode))
	c.statusMessage.Append(s.StatusMessage)

	c.batchID.Append(s.BatchID)
	c.attributes.Append(s.Attrs)
	c.resource.Append(s.ResourceAttrs)
	{
		m := s.ResourceAttrs.AsMap()
		setStrOrEmpty(c.serviceInstanceID, m, string(semconv.ServiceInstanceIDKey))
		setStrOrEmpty(c.serviceName, m, string(semconv.ServiceNameKey))
		setStrOrEmpty(c.serviceNamespace, m, string(semconv.ServiceNamespaceKey))
	}
	c.scopeName.Append(s.ScopeName)
	c.scopeVersion.Append(s.ScopeVersion)
	c.scopeAttributes.Append(s.ScopeAttrs)

	c.events.AddRow(s.Events)
	c.links.AddRow(s.Links)
}

func (c *spanColumns) Row(i int) (s tracestorage.Span, _ error) {
	attrs := c.attributes.Row(i)
	resource := c.resource.Row(i)
	{
		v := resource.AsMap()
		if s := c.serviceInstanceID.Row(i); s != "" {
			v.PutStr(string(semconv.ServiceInstanceIDKey), s)
		}
		if s := c.serviceName.Row(i); s != "" {
			v.PutStr(string(semconv.ServiceNameKey), s)
		}
		if s := c.serviceNamespace.Row(i); s != "" {
			v.PutStr(string(semconv.ServiceNamespaceKey), s)
		}
	}
	scopeAttrs := c.scopeAttributes.Row(i)
	events, err := c.events.Row(i)
	if err != nil {
		return s, errors.Wrap(err, "decode events")
	}
	links, err := c.links.Row(i)
	if err != nil {
		return s, errors.Wrap(err, "decode links")
	}

	return tracestorage.Span{
		TraceID:       c.traceID.Row(i),
		SpanID:        c.spanID.Row(i),
		TraceState:    c.traceState.Row(i),
		ParentSpanID:  c.parentSpanID.Row(i),
		Name:          c.name.Row(i),
		Kind:          int32(c.kind.Row(i)),
		Start:         otelstorage.NewTimestampFromTime(c.start.Row(i)),
		End:           otelstorage.NewTimestampFromTime(c.end.Row(i)),
		Attrs:         attrs,
		StatusCode:    int32(c.statusCode.Row(i)),
		StatusMessage: c.statusMessage.Row(i),
		BatchID:       c.batchID.Row(i),
		ResourceAttrs: resource,
		ScopeName:     c.scopeName.Row(i),
		ScopeVersion:  c.scopeVersion.Row(i),
		ScopeAttrs:    scopeAttrs,
		Events:        events,
		Links:         links,
	}, nil
}

func (c *spanColumns) ReadRowsTo(spans []tracestorage.Span) ([]tracestorage.Span, error) {
	for i := 0; i < c.traceID.Rows(); i++ {
		span, err := c.Row(i)
		if err != nil {
			return nil, err
		}
		spans = append(spans, span)
	}

	return spans, nil
}

func (c *spanColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:     ddl.Engine{Type: "MergeTree"},
		PrimaryKey: []string{"service_namespace", "service_name", "resource"},
		OrderBy:    []string{"service_namespace", "service_name", "resource", "start"},
		TTL:        ddl.TTL{Field: "start"},
		Indexes: []ddl.Index{
			{
				Name:        "idx_trace_id",
				Target:      "trace_id",
				Type:        "bloom_filter",
				Params:      []string{"0.001"},
				Granularity: 1,
			},
		},
		Columns: []ddl.Column{
			{
				Name:    "service_instance_id",
				Type:    c.serviceInstanceID.Type(),
				Comment: "service.instance.id",
			},
			{
				Name:    "service_name",
				Type:    c.serviceName.Type(),
				Comment: "service.name",
			},
			{
				Name:    "service_namespace",
				Type:    c.serviceNamespace.Type(),
				Comment: "service.namespace",
			},
			{
				Name: "trace_id",
				Type: c.traceID.Type(),
			},
			{
				Name: "span_id",
				Type: c.spanID.Type(),
			},
			{
				Name: "trace_state",
				Type: c.traceState.Type(),
			},
			{
				Name: "parent_span_id",
				Type: c.parentSpanID.Type(),
			},
			{
				Name: "name",
				Type: c.name.Type(),
			},
			{
				Name: "kind",
				Type: c.kind.Type().Sub(kindDDL),
			},
			{
				Name:  "start",
				Type:  c.start.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name:  "end",
				Type:  c.end.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name:         "duration_ns",
				Type:         proto.ColumnTypeUInt64,
				Materialized: "toUnixTimestamp64Nano(end)-toUnixTimestamp64Nano(start)",
				Codec:        "T64, ZSTD(1)",
			},
			{
				Name: "status_code",
				Type: c.statusCode.Type(),
			},
			{
				Name: "status_message",
				Type: c.statusMessage.Type(),
			},
			{
				Name: "batch_id",
				Type: c.batchID.Type(),
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)
	c.scopeAttributes.DDL(&table)

	table.Columns = append(table.Columns, []ddl.Column{
		{
			Name: "scope_name",
			Type: c.scopeName.Type(),
		},
		{
			Name: "scope_version",
			Type: c.scopeVersion.Type(),
		},
		{
			Name: "events_timestamps",
			Type: c.events.timestamps.Type(),
		},
		{
			Name: "events_names",
			Type: c.events.names.Type(),
		},
		{
			Name: "events_attributes",
			Type: c.events.attributes.Type(),
		},
		{
			Name: "links_trace_ids",
			Type: c.links.traceIDs.Type(),
		},
		{
			Name: "links_span_ids",
			Type: c.links.spanIDs.Type(),
		},
		{
			Name: "links_tracestates",
			Type: c.links.tracestates.Type(),
		},
		{
			Name: "links_attributes",
			Type: c.links.attributes.Type(),
		},
	}...)

	return table
}

type eventsColumns struct {
	names      *proto.ColArr[string]
	timestamps *proto.ColArr[time.Time]
	attributes *proto.ColArr[[]byte]
}

func newEventsColumns() eventsColumns {
	return eventsColumns{
		names:      new(proto.ColStr).Array(),
		timestamps: new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano).Array(),
		attributes: new(proto.ColBytes).Array(),
	}
}

func (c *eventsColumns) AddRow(events []tracestorage.Event) {
	var (
		names      []string
		timestamps []time.Time
		attrs      [][]byte
	)
	for _, e := range events {
		names = append(names, e.Name)
		timestamps = append(timestamps, time.Unix(0, int64(e.Timestamp)))
		attrs = append(attrs, encodeAttributes(e.Attrs.AsMap()))
	}

	c.names.Append(names)
	c.timestamps.Append(timestamps)
	c.attributes.Append(attrs)
}

func (c *eventsColumns) Row(row int) (events []tracestorage.Event, _ error) {
	var (
		names      = c.names.Row(row)
		timestamps = c.timestamps.Row(row)
		attributes = c.attributes.Row(row)

		l = min(
			len(names),
			len(timestamps),
			len(attributes),
		)
	)
	for i := 0; i < l; i++ {
		attrs, err := decodeAttributes(attributes[i])
		if err != nil {
			return nil, errors.Wrap(err, "decode attributes")
		}

		events = append(events, tracestorage.Event{
			Name:      names[i],
			Timestamp: otelstorage.NewTimestampFromTime(timestamps[i]),
			Attrs:     attrs,
		})
	}
	return events, nil
}

type linksColumns struct {
	traceIDs    *proto.ColArr[otelstorage.TraceID]
	spanIDs     *proto.ColArr[otelstorage.SpanID]
	tracestates *proto.ColArr[string]
	attributes  *proto.ColArr[[]byte]
}

func newLinksColumns() linksColumns {
	return linksColumns{
		traceIDs:    proto.NewArray[otelstorage.TraceID](&proto.ColRawOf[otelstorage.TraceID]{}),
		spanIDs:     proto.NewArray[otelstorage.SpanID](&proto.ColRawOf[otelstorage.SpanID]{}),
		tracestates: new(proto.ColStr).Array(),
		attributes:  new(proto.ColBytes).Array(),
	}
}

func (c *linksColumns) AddRow(links []tracestorage.Link) {
	var (
		traceIDs    []otelstorage.TraceID
		spanIDs     []otelstorage.SpanID
		tracestates []string
		attributes  [][]byte
	)
	for _, l := range links {
		traceIDs = append(traceIDs, l.TraceID)
		spanIDs = append(spanIDs, l.SpanID)
		tracestates = append(tracestates, l.TraceState)
		attributes = append(attributes, encodeAttributes(l.Attrs.AsMap()))
	}

	c.traceIDs.Append(traceIDs)
	c.spanIDs.Append(spanIDs)
	c.tracestates.Append(tracestates)
	c.attributes.Append(attributes)
}

func (c *linksColumns) Row(row int) (links []tracestorage.Link, _ error) {
	var (
		traceIDs    = c.traceIDs.Row(row)
		spanIDs     = c.spanIDs.Row(row)
		tracestates = c.tracestates.Row(row)
		attributes  = c.attributes.Row(row)

		l = min(
			len(traceIDs),
			len(spanIDs),
			len(tracestates),
			len(attributes),
		)
	)
	for i := 0; i < l; i++ {
		attrs, err := decodeAttributes(attributes[i])
		if err != nil {
			return nil, errors.Wrap(err, "decode attributes")
		}

		links = append(links, tracestorage.Link{
			TraceID:    traceIDs[i],
			SpanID:     spanIDs[i],
			TraceState: tracestates[i],
			Attrs:      attrs,
		})
	}
	return links, nil
}

type spanAttrsColumns struct {
	name      *proto.ColLowCardinality[string]
	value     proto.ColStr
	valueType proto.ColEnum8
	scope     proto.ColEnum8

	columns func() Columns
	Input   func() proto.Input
	Body    func(table string) string
}

func newSpanAttrsColumns() *spanAttrsColumns {
	c := &spanAttrsColumns{
		name:      new(proto.ColStr).LowCardinality(),
		value:     proto.ColStr{},
		valueType: proto.ColEnum8{},
		scope:     proto.ColEnum8{},
	}
	c.columns = sync.OnceValue(func() Columns {
		return Columns{
			{Name: "name", Data: c.name},
			{Name: "value", Data: &c.value},
			{Name: "value_type", Data: proto.Wrap(&c.valueType, valueTypeDDL)},
			{Name: "scope", Data: proto.Wrap(&c.scope, scopeTypeDDL)},
		}
	})
	c.Input = sync.OnceValue(func() proto.Input {
		return c.columns().Input()
	})
	c.Body = xsync.KeyOnce(func(table string) string {
		return c.Input().Into(table)
	})
	return c
}

func (c *spanAttrsColumns) Result() proto.Results             { return c.columns().Result() }
func (c *spanAttrsColumns) ChsqlResult() []chsql.ResultColumn { return c.columns().ChsqlResult() }
func (c *spanAttrsColumns) Reset()                            { c.columns().Reset() }

func (c *spanAttrsColumns) AddAttrs(scope traceql.AttributeScope, attrs otelstorage.Attrs) {
	attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
		c.AddRow(tracestorage.TagFromAttribute(scope, k, v))
		return true
	})
}

func (c *spanAttrsColumns) AddRow(tag tracestorage.Tag) {
	c.name.Append(tag.Name)
	c.value.Append(tag.Value)
	c.valueType.Append(proto.Enum8(tag.Type))
	c.scope.Append(proto.Enum8(tag.Scope))
}
