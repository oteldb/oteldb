package otlpdirect_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// FuzzConvertTraces drives the span decoder with arbitrary bytes. Traces is the deepest of the
// signals — a span nests status, events and links, each with their own attributes, each of which
// can nest maps and slices to any depth — so this is where a malformed request has the most room
// to find an unchecked length or an unbounded recursion.
func FuzzConvertTraces(f *testing.F) {
	seed := func(build func(ptrace.Traces)) {
		td := ptrace.NewTraces()
		build(td)

		raw, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if err != nil {
			f.Fatal(err)
		}

		f.Add(raw)
	}

	seed(func(ptrace.Traces) {})

	seed(func(td ptrace.Traces) {
		sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetName("op")
		sp.SetStartTimestamp(1)
		sp.SetEndTimestamp(2)
	})

	seed(func(td ptrace.Traces) {
		rs := td.ResourceSpans().AppendEmpty()
		rs.SetSchemaUrl("res")
		rs.Resource().Attributes().PutStr("service.name", "api")

		ss := rs.ScopeSpans().AppendEmpty()
		ss.SetSchemaUrl("scope")
		ss.Scope().SetName("n")
		ss.Scope().SetVersion("1")

		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID([16]byte{1, 2, 3})
		sp.SetSpanID([8]byte{4})
		sp.SetParentSpanID([8]byte{5})
		sp.TraceState().FromRaw("k=v")
		sp.SetName("GET /x")
		sp.SetKind(ptrace.SpanKindClient)
		sp.SetStartTimestamp(1)
		sp.SetEndTimestamp(2)
		sp.SetFlags(1)
		sp.SetDroppedAttributesCount(1)
		sp.Status().SetCode(ptrace.StatusCodeError)
		sp.Status().SetMessage("boom")

		nested := sp.Attributes().PutEmptyMap("nested")
		nested.PutStr("a", "b")
		nested.PutEmptySlice("l").AppendEmpty().SetDouble(1.5)
		sp.Attributes().PutEmptyBytes("raw").Append(1, 2, 3)

		ev := sp.Events().AppendEmpty()
		ev.SetTimestamp(3)
		ev.SetName("ev")
		ev.SetDroppedAttributesCount(2)
		ev.Attributes().PutInt("i", 1)

		ln := sp.Links().AppendEmpty()
		ln.SetTraceID([16]byte{9})
		ln.SetSpanID([8]byte{9})
		ln.TraceState().FromRaw("l=s")
		ln.SetDroppedAttributesCount(3)
		ln.Attributes().PutBool("b", true)
	})

	// Many spans in one scope, so the reused collectors are exercised under mutation.
	seed(func(td ptrace.Traces) {
		ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		for i := range 8 {
			sp := ss.Spans().AppendEmpty()
			sp.SetName("op")
			sp.SetStartTimestamp(1)
			sp.Attributes().PutInt("i", int64(i))

			if i%2 == 0 {
				sp.Events().AppendEmpty().SetName("ev")
			}

			if i%3 == 0 {
				sp.Links().AppendEmpty().SetTraceID([16]byte{byte(i)})
			}
		}
	})

	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var c otlpdirect.TracesConverter

		got, err := c.Convert(data)
		if err != nil {
			return
		}

		for i := range got.Resources {
			rs := &got.Resources[i]
			_ = len(rs.Resource.SchemaURL) + len(rs.Resource.Attributes)

			for j := range rs.Scopes {
				ss := &rs.Scopes[j]
				_ = len(ss.Scope.Name)

				for k := range ss.Spans {
					sp := &ss.Spans[k]
					_ = len(sp.Name) + len(sp.TraceID) + len(sp.SpanID) + len(sp.ParentSpanID) +
						len(sp.TraceState) + len(sp.StatusMessage) + len(sp.Attributes)

					for e := range sp.Events {
						_ = len(sp.Events[e].Name) + len(sp.Events[e].Attributes)
					}

					for l := range sp.Links {
						_ = len(sp.Links[l].TraceID) + len(sp.Links[l].Attributes)
					}
				}
			}
		}

		// The collectors are reused across spans and across calls; a second pass over the same
		// bytes must produce the same batch, not one contaminated by the first.
		again, err := c.Convert(data)
		if err != nil {
			t.Fatalf("second convert of the same input failed: %v", err)
		}

		if len(again.Resources) != len(got.Resources) {
			t.Fatalf("reuse changed the batch: %d resources then %d", len(got.Resources), len(again.Resources))
		}
	})
}
