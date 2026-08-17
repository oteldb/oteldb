package otlpdirect_test

import (
	"github.com/VictoriaMetrics/easyproto"

	"github.com/oteldb/storage/signal/trace"
)

// encodeScopeSpans builds one ExportTraceServiceRequest carrying a single fully-populated span,
// writing each message's fields ascending or descending. Traces has more nesting than logs — a
// span's status, events and links are all submessages siblings of scalar fields — so the ordering
// hazard has more places to hide here.
func encodeScopeSpans(ascending bool) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rs := req.AppendMessage(1) // resource_spans

	writeResource := func() {
		res := rs.AppendMessage(1)
		kv := res.AppendMessage(1)
		kv.AppendString(1, "service.name")
		kv.AppendMessage(2).AppendString(1, "api")
	}

	writeScopes := func() {
		ss := rs.AppendMessage(2) // scope_spans

		writeScope := func() {
			sc := ss.AppendMessage(1)
			sc.AppendString(1, "scope-name")
			sc.AppendString(2, "1.0.0")
		}

		writeSpans := func() {
			sp := ss.AppendMessage(2) // spans

			writeIDs := func() {
				sp.AppendBytes(1, []byte("0123456789abcdef")) // trace_id
				sp.AppendBytes(2, []byte("01234567"))         // span_id
				sp.AppendString(3, "vendor=v")                // trace_state
				sp.AppendBytes(4, []byte("76543210"))         // parent_span_id
			}

			writeCore := func() {
				sp.AppendString(5, "op")                       // name
				sp.AppendInt64(6, 2)                           // kind
				sp.AppendFixed64(7, 1_700_000_000_000_000_000) // start
				sp.AppendFixed64(8, 1_700_000_000_500_000_000) // end
				kv := sp.AppendMessage(9)                      // attributes
				kv.AppendString(1, "http.method")              //
				kv.AppendMessage(2).AppendString(1, "GET")     //
				sp.AppendInt64(10, 2)                          // dropped_attributes_count
				sp.AppendFixed32(16, 1)                        // flags
			}

			writeEvent := func() {
				ev := sp.AppendMessage(11)
				ev.AppendFixed64(1, 1_700_000_000_100_000_000)
				ev.AppendString(2, "exception")

				kv := ev.AppendMessage(3)
				kv.AppendString(1, "exception.type")
				kv.AppendMessage(2).AppendString(1, "TimeoutError")
			}

			writeLink := func() {
				ln := sp.AppendMessage(13)
				ln.AppendBytes(1, []byte("fedcba9876543210"))
				ln.AppendBytes(2, []byte("76543210"))
				ln.AppendString(3, "link=state")

				kv := ln.AppendMessage(4)
				kv.AppendString(1, "rel")
				kv.AppendMessage(2).AppendString(1, "follows_from")
			}

			writeStatus := func() {
				st := sp.AppendMessage(15)
				st.AppendString(2, "upstream timeout")
				st.AppendInt64(3, 2)
			}

			if ascending {
				writeIDs()
				writeCore()
				writeEvent()
				writeLink()
				writeStatus()
			} else {
				writeStatus()
				writeLink()
				writeEvent()
				writeCore()
				writeIDs()
			}
		}

		writeSchema := func() { ss.AppendString(3, "https://schema.example/scope") }

		if ascending {
			writeScope()
			writeSpans()
			writeSchema()
		} else {
			writeSchema()
			writeSpans()
			writeScope()
		}
	}

	writeSchema := func() { rs.AppendString(3, "https://schema.example/resource") }

	if ascending {
		writeResource()
		writeScopes()
		writeSchema()
	} else {
		writeSchema()
		writeScopes()
		writeResource()
	}

	return m.Marshal(nil)
}

// canonicalTraces is [canonical] for the traces model: it rewrites zero-length byte slices to nil
// so the two paths' spellings of an absent field compare equal. See canonical_test.go.
func canonicalTraces(t *trace.Traces) *trace.Traces {
	for i := range t.Resources {
		rs := &t.Resources[i]
		rs.Resource.SchemaURL = canonBytes(rs.Resource.SchemaURL)
		rs.Resource.Attributes = canonAttrs(rs.Resource.Attributes)

		for j := range rs.Scopes {
			ss := &rs.Scopes[j]
			ss.Scope.Name = canonBytes(ss.Scope.Name)
			ss.Scope.Version = canonBytes(ss.Scope.Version)
			ss.Scope.SchemaURL = canonBytes(ss.Scope.SchemaURL)
			ss.Scope.Attributes = canonAttrs(ss.Scope.Attributes)

			for k := range ss.Spans {
				canonicalSpan(&ss.Spans[k])
			}

			if len(ss.Spans) == 0 {
				ss.Spans = nil
			}
		}
	}

	return t
}

func canonicalSpan(sp *trace.Span) {
	sp.TraceID = canonBytes(sp.TraceID)
	sp.SpanID = canonBytes(sp.SpanID)
	sp.ParentSpanID = canonBytes(sp.ParentSpanID)
	sp.Name = canonBytes(sp.Name)
	sp.StatusMessage = canonBytes(sp.StatusMessage)
	sp.TraceState = canonBytes(sp.TraceState)
	sp.Attributes = canonAttrs(sp.Attributes)

	for i := range sp.Events {
		e := &sp.Events[i]
		e.Name = canonBytes(e.Name)
		e.Attributes = canonAttrs(e.Attributes)
	}

	for i := range sp.Links {
		l := &sp.Links[i]
		l.TraceID = canonBytes(l.TraceID)
		l.SpanID = canonBytes(l.SpanID)
		l.TraceState = canonBytes(l.TraceState)
		l.Attributes = canonAttrs(l.Attributes)
	}

	if len(sp.Events) == 0 {
		sp.Events = nil
	}

	if len(sp.Links) == 0 {
		sp.Links = nil
	}
}
