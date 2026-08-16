package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// Field numbers of trace.proto and collector/trace/v1/trace_service.proto.
const (
	// opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
	fieldExportResourceSpans = 1

	// opentelemetry.proto.trace.v1.ResourceSpans
	fieldResourceSpansResource  = 1
	fieldResourceSpansScope     = 2
	fieldResourceSpansSchemaURL = 3

	// opentelemetry.proto.trace.v1.ScopeSpans
	fieldScopeSpansScope     = 1
	fieldScopeSpansSpans     = 2
	fieldScopeSpansSchemaURL = 3

	// opentelemetry.proto.trace.v1.Span
	fieldSpanTraceID      = 1
	fieldSpanSpanID       = 2
	fieldSpanTraceState   = 3
	fieldSpanParentSpanID = 4
	fieldSpanName         = 5
	fieldSpanKind         = 6
	fieldSpanStart        = 7
	fieldSpanEnd          = 8
	fieldSpanAttributes   = 9
	fieldSpanDropped      = 10
	fieldSpanEvents       = 11
	fieldSpanLinks        = 13
	fieldSpanStatus       = 15
	fieldSpanFlags        = 16

	// opentelemetry.proto.trace.v1.Status
	fieldStatusMessage = 2
	fieldStatusCode    = 3

	// opentelemetry.proto.trace.v1.Span.Event
	fieldEventTime       = 1
	fieldEventName       = 2
	fieldEventAttributes = 3
	fieldEventDropped    = 4

	// opentelemetry.proto.trace.v1.Span.Link
	fieldLinkTraceID    = 1
	fieldLinkSpanID     = 2
	fieldLinkTraceState = 3
	fieldLinkAttributes = 4
	fieldLinkDropped    = 5
)

// TracesConverter decodes an OTLP ExportTraceServiceRequest into [trace.Traces]. It retains the
// batch and the scratch it is built from, so a converter reused across requests allocates nothing
// in steady state. It is not safe for concurrent use; pool one per in-flight request.
type TracesConverter struct {
	batch trace.Traces
	dec   decoder

	// Submessage collectors, reused across the spans of a request. Each is consumed before the
	// next span reaches it, so one buffer per kind serves the whole walk.
	spanAttrs  [][]byte
	spanEvents [][]byte
	spanLinks  [][]byte
	subAttrs   [][]byte
}

// Convert decodes a serialized ExportTraceServiceRequest.
//
// The returned batch aliases src: every key, name, id and string value is a sub-slice of it. It
// stays valid until the next Convert on this converter, and src must not be recycled until the
// write consuming the batch has returned.
func (c *TracesConverter) Convert(src []byte) (*trace.Traces, error) {
	c.batch.Reset()
	c.dec.reset()

	resources, err := collect(src, fieldExportResourceSpans, "resource spans")
	if err != nil {
		return nil, err
	}

	for _, data := range resources {
		if err := c.resourceSpans(data); err != nil {
			return nil, err
		}
	}

	return &c.batch, nil
}

func (c *TracesConverter) resourceSpans(src []byte) error {
	var (
		fc     easyproto.FieldContext
		res    signal.Resource
		scopes [][]byte
		err    error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read resource spans field")
		}

		switch fc.FieldNum {
		case fieldResourceSpansResource:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read resource")
			}

			if res.Attributes, err = c.dec.resource(data); err != nil {
				return err
			}
		case fieldResourceSpansScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope spans")
			}

			scopes = append(scopes, data)
		case fieldResourceSpansSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read resource schema url")
			}

			res.SchemaURL = v
		}
	}

	rs := c.batch.AddResource()
	rs.Resource = res

	for _, data := range scopes {
		if err := c.scopeSpans(rs, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *TracesConverter) scopeSpans(rs *trace.ResourceSpans, src []byte) error {
	var (
		fc        easyproto.FieldContext
		scopeData []byte
		schemaURL []byte
		spans     [][]byte
		err       error
	)

	// Field order is the producer's choice — pdata writes them descending, so schema_url arrives
	// before scope. The scope submessage is therefore decoded after the walk, never during it:
	// decoding in place would overwrite a schema_url already read.
	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read scope spans field")
		}

		switch fc.FieldNum {
		case fieldScopeSpansScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope")
			}

			scopeData = data
		case fieldScopeSpansSpans:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read span")
			}

			spans = append(spans, data)
		case fieldScopeSpansSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read scope schema url")
			}

			schemaURL = v
		}
	}

	sc, err := c.dec.scope(scopeData)
	if err != nil {
		return err
	}

	sc.SchemaURL = schemaURL

	ss := rs.AddScope()
	ss.Scope = sc

	for _, data := range spans {
		if err := c.span(ss, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *TracesConverter) span(ss *trace.ScopeSpans, src []byte) error {
	var (
		fc         easyproto.FieldContext
		statusData []byte
		err        error
	)

	sp := ss.AddSpan()

	attrs := c.spanAttrs[:0]
	events := c.spanEvents[:0]
	links := c.spanLinks[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read span field")
		}

		switch fc.FieldNum {
		case fieldSpanTraceID:
			if sp.TraceID, err = takeBytes(&fc, "span trace id"); err != nil {
				return err
			}
		case fieldSpanSpanID:
			if sp.SpanID, err = takeBytes(&fc, "span id"); err != nil {
				return err
			}
		case fieldSpanParentSpanID:
			if sp.ParentSpanID, err = takeBytes(&fc, "parent span id"); err != nil {
				return err
			}
		case fieldSpanTraceState:
			if sp.TraceState, err = takeBytes(&fc, "span trace state"); err != nil {
				return err
			}
		case fieldSpanName:
			if sp.Name, err = takeBytes(&fc, "span name"); err != nil {
				return err
			}
		case fieldSpanKind:
			v, ok := fc.Enum()
			if !ok {
				return errors.New("read span kind")
			}

			sp.Kind = v
		case fieldSpanStart:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read span start")
			}

			sp.Start = int64(v)
		case fieldSpanEnd:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read span end")
			}

			sp.End = int64(v)
		case fieldSpanAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read span attribute")
			}

			attrs = append(attrs, data)
		case fieldSpanDropped:
			v, ok := fc.Uint32()
			if !ok {
				return errors.New("read span dropped count")
			}

			sp.Dropped = v
		case fieldSpanEvents:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read span event")
			}

			events = append(events, data)
		case fieldSpanLinks:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read span link")
			}

			links = append(links, data)
		case fieldSpanStatus:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read span status")
			}

			statusData = data
		case fieldSpanFlags:
			v, ok := fc.Fixed32()
			if !ok {
				return errors.New("read span flags")
			}

			sp.Flags = v
		}
	}

	c.spanAttrs, c.spanEvents, c.spanLinks = attrs, events, links

	if sp.Attributes, err = c.dec.attributes(attrs); err != nil {
		return err
	}

	if err := c.status(sp, statusData); err != nil {
		return err
	}

	for _, data := range events {
		if err := c.event(sp, data); err != nil {
			return err
		}
	}

	for _, data := range links {
		if err := c.link(sp, data); err != nil {
			return err
		}
	}

	return nil
}

// status decodes a Span.Status. An absent status leaves the zero code and message, which is what
// the pdata path produces for a span that never set one.
func (c *TracesConverter) status(sp *trace.Span, src []byte) error {
	var (
		fc  easyproto.FieldContext
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read status field")
		}

		switch fc.FieldNum {
		case fieldStatusMessage:
			if sp.StatusMessage, err = takeBytes(&fc, "status message"); err != nil {
				return err
			}
		case fieldStatusCode:
			v, ok := fc.Enum()
			if !ok {
				return errors.New("read status code")
			}

			sp.StatusCode = v
		}
	}

	return nil
}

func (c *TracesConverter) event(sp *trace.Span, src []byte) error {
	var (
		fc  easyproto.FieldContext
		ev  trace.Event
		err error
	)

	kvs := c.subAttrs[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read event field")
		}

		switch fc.FieldNum {
		case fieldEventTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read event time")
			}

			ev.Time = int64(v)
		case fieldEventName:
			if ev.Name, err = takeBytes(&fc, "event name"); err != nil {
				return err
			}
		case fieldEventAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read event attribute")
			}

			kvs = append(kvs, data)
		case fieldEventDropped:
			v, ok := fc.Uint32()
			if !ok {
				return errors.New("read event dropped count")
			}

			ev.Dropped = v
		}
	}

	c.subAttrs = kvs

	if ev.Attributes, err = c.dec.attributes(kvs); err != nil {
		return err
	}

	*sp.AddEvent() = ev

	return nil
}

func (c *TracesConverter) link(sp *trace.Span, src []byte) error {
	var (
		fc  easyproto.FieldContext
		ln  trace.Link
		err error
	)

	kvs := c.subAttrs[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read link field")
		}

		switch fc.FieldNum {
		case fieldLinkTraceID:
			if ln.TraceID, err = takeBytes(&fc, "link trace id"); err != nil {
				return err
			}
		case fieldLinkSpanID:
			if ln.SpanID, err = takeBytes(&fc, "link span id"); err != nil {
				return err
			}
		case fieldLinkTraceState:
			if ln.TraceState, err = takeBytes(&fc, "link trace state"); err != nil {
				return err
			}
		case fieldLinkAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read link attribute")
			}

			kvs = append(kvs, data)
		case fieldLinkDropped:
			v, ok := fc.Uint32()
			if !ok {
				return errors.New("read link dropped count")
			}

			ln.Dropped = v
		}
	}

	c.subAttrs = kvs

	if ln.Attributes, err = c.dec.attributes(kvs); err != nil {
		return err
	}

	*sp.AddLink() = ln

	return nil
}

// takeBytes reads a length-delimited field, naming it in any error.
func takeBytes(fc *easyproto.FieldContext, what string) ([]byte, error) {
	v, ok := fc.Bytes()
	if !ok {
		return nil, errors.Errorf("read %s", what)
	}

	return v, nil
}
