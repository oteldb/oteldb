package tracestorage

import (
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// NewSpanFromOTEL creates new Span.
func NewSpanFromOTEL(
	batchID uuid.UUID,
	res pcommon.Resource,
	scope pcommon.InstrumentationScope,
	span ptrace.Span,
) (s Span) {
	status := span.Status()
	s = Span{
		TraceID:       otelstorage.TraceID(span.TraceID()),
		SpanID:        otelstorage.SpanID(span.SpanID()),
		TraceState:    span.TraceState().AsRaw(),
		ParentSpanID:  otelstorage.SpanID(span.ParentSpanID()),
		Name:          span.Name(),
		Kind:          int32(span.Kind()),
		Start:         span.StartTimestamp(),
		End:           span.EndTimestamp(),
		Attrs:         otelstorage.Attrs(span.Attributes()),
		StatusCode:    int32(status.Code()),
		StatusMessage: status.Message(),
		BatchID:       batchID,
		ResourceAttrs: otelstorage.Attrs(res.Attributes()),
		ScopeName:     scope.Name(),
		ScopeVersion:  scope.Version(),
		ScopeAttrs:    otelstorage.Attrs(scope.Attributes()),
		Events:        nil,
		Links:         nil,
	}
	if events := span.Events(); events.Len() > 0 {
		s.Events = make([]Event, 0, events.Len())
		for i := 0; i < events.Len(); i++ {
			event := events.At(i)
			s.Events = append(s.Events, Event{
				Timestamp: event.Timestamp(),
				Name:      event.Name(),
				Attrs:     otelstorage.Attrs(event.Attributes()),
			})
		}
	}
	if links := span.Links(); links.Len() > 0 {
		s.Links = make([]Link, 0, links.Len())
		for i := 0; i < links.Len(); i++ {
			link := links.At(i)
			s.Links = append(s.Links, Link{
				TraceID:    otelstorage.TraceID(link.TraceID()),
				SpanID:     otelstorage.SpanID(link.SpanID()),
				TraceState: link.TraceState().AsRaw(),
				Attrs:      otelstorage.Attrs(link.Attributes()),
			})
		}
	}

	return s
}

// FillOTELSpan fills given OpenTelemetry span using span fields.
func (span Span) FillOTELSpan(s ptrace.Span) {
	s.SetTraceID(pcommon.TraceID(span.TraceID))
	s.SetSpanID(pcommon.SpanID(span.SpanID))
	s.TraceState().FromRaw(span.TraceState)
	if p := span.ParentSpanID; !p.IsEmpty() {
		s.SetParentSpanID(pcommon.SpanID(p))
	}
	s.SetName(span.Name)
	s.SetKind(ptrace.SpanKind(span.Kind))
	s.SetStartTimestamp(span.Start)
	s.SetEndTimestamp(span.End)
	span.Attrs.CopyTo(s.Attributes())

	status := s.Status()
	status.SetCode(ptrace.StatusCode(span.StatusCode))
	status.SetMessage(span.StatusMessage)

	mapSpanEvents(span.Events, s.Events())
	mapSpanLinks(span.Links, s.Links())
}

func mapSpanEvents(events []Event, to ptrace.SpanEventSlice) {
	for _, event := range events {
		e := to.AppendEmpty()
		e.SetTimestamp(event.Timestamp)
		e.SetName(event.Name)
		event.Attrs.CopyTo(e.Attributes())
	}
}

func mapSpanLinks(links []Link, to ptrace.SpanLinkSlice) {
	for _, link := range links {
		l := to.AppendEmpty()
		l.SetTraceID(pcommon.TraceID(link.TraceID))
		l.SetSpanID(pcommon.SpanID(link.SpanID))
		l.TraceState().FromRaw(link.TraceState)
		link.Attrs.CopyTo(l.Attributes())
	}
}
