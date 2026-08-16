package otlpdirect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

func marshalTraces(tb testing.TB, td ptrace.Traces) []byte {
	tb.Helper()

	raw, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	require.NoError(tb, err)

	return raw
}

// convertBothTraces decodes td directly and via the pdata path, canonicalized for comparison.
func convertBothTraces(tb testing.TB, td ptrace.Traces) (direct, viaPdata *trace.Traces) {
	tb.Helper()

	var c otlpdirect.TracesConverter

	direct, err := c.Convert(marshalTraces(tb, td))
	require.NoError(tb, err)

	viaPdata = &trace.Traces{}
	require.Zero(tb, pdataconv.AppendTraces(viaPdata, td))

	return canonicalTraces(direct), canonicalTraces(viaPdata)
}

// TestConvertTracesMatchesPdata exercises every field of a span, including the nested status,
// events and links, against the pdata path.
func TestConvertTracesMatchesPdata(t *testing.T) {
	t.Parallel()

	td := ptrace.NewTraces()

	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://schema.example/resource")
	rs.Resource().Attributes().PutStr("service.name", "api")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.SetSchemaUrl("https://schema.example/scope")
	ss.Scope().SetName("go.opentelemetry.io/example")
	ss.Scope().SetVersion("1.2.3")
	ss.Scope().Attributes().PutBool("experimental", true)

	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sp.SetParentSpanID([8]byte{8, 7, 6, 5, 4, 3, 2, 1})
	sp.TraceState().FromRaw("vendor=value")
	sp.SetName("GET /things")
	sp.SetKind(ptrace.SpanKindServer)
	sp.SetStartTimestamp(1_700_000_000_000_000_000)
	sp.SetEndTimestamp(1_700_000_000_500_000_000)
	sp.SetFlags(1)
	sp.SetDroppedAttributesCount(2)
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.Status().SetMessage("upstream timeout")
	sp.Attributes().PutStr("http.method", "GET")
	sp.Attributes().PutInt("http.status_code", 504)

	ev := sp.Events().AppendEmpty()
	ev.SetTimestamp(1_700_000_000_100_000_000)
	ev.SetName("exception")
	ev.SetDroppedAttributesCount(1)
	ev.Attributes().PutStr("exception.type", "TimeoutError")

	ev2 := sp.Events().AppendEmpty()
	ev2.SetTimestamp(1_700_000_000_200_000_000)
	ev2.SetName("retry")

	ln := sp.Links().AppendEmpty()
	ln.SetTraceID([16]byte{9})
	ln.SetSpanID([8]byte{9})
	ln.TraceState().FromRaw("link=state")
	ln.SetDroppedAttributesCount(3)
	ln.Attributes().PutStr("rel", "follows_from")

	direct, viaPdata := convertBothTraces(t, td)
	assert.Equal(t, viaPdata, direct)
}

// TestConvertTracesMinimalSpan covers the span a real tracer emits with nothing optional set — no
// status, no events, no links, no attributes.
func TestConvertTracesMinimalSpan(t *testing.T) {
	t.Parallel()

	td := ptrace.NewTraces()

	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("op")
	sp.SetStartTimestamp(1)
	sp.SetEndTimestamp(2)

	direct, viaPdata := convertBothTraces(t, td)
	require.Equal(t, viaPdata, direct)

	got := direct.Resources[0].Scopes[0].Spans[0]
	assert.Empty(t, got.Events)
	assert.Empty(t, got.Links)
	assert.Zero(t, got.StatusCode)
}

// TestConvertTracesSpanKindsAndStatuses walks the enums, where an Enum/Uint32 mix-up hides.
func TestConvertTracesSpanKindsAndStatuses(t *testing.T) {
	t.Parallel()

	for _, kind := range []ptrace.SpanKind{
		ptrace.SpanKindUnspecified, ptrace.SpanKindInternal, ptrace.SpanKindServer,
		ptrace.SpanKindClient, ptrace.SpanKindProducer, ptrace.SpanKindConsumer,
	} {
		for _, code := range []ptrace.StatusCode{
			ptrace.StatusCodeUnset, ptrace.StatusCodeOk, ptrace.StatusCodeError,
		} {
			td := ptrace.NewTraces()

			sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetName("op")
			sp.SetStartTimestamp(1)
			sp.SetKind(kind)
			sp.Status().SetCode(code)

			direct, viaPdata := convertBothTraces(t, td)
			require.Equal(t, viaPdata, direct, "kind=%v code=%v", kind, code)
		}
	}
}

// TestConvertTracesManySpans pins that the collectors reused across spans do not bleed one span's
// attributes, events or links into the next.
func TestConvertTracesManySpans(t *testing.T) {
	t.Parallel()

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()

	for i := range 20 {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("op")
		sp.SetStartTimestamp(pcommon.Timestamp(i + 1))
		sp.Attributes().PutInt("i", int64(i))

		// Only some spans carry events and links, so a leaked collector would show up as a span
		// inheriting its predecessor's.
		if i%3 == 0 {
			e := sp.Events().AppendEmpty()
			e.SetName("ev")
			e.Attributes().PutInt("i", int64(i))
		}

		if i%4 == 0 {
			l := sp.Links().AppendEmpty()
			l.SetTraceID([16]byte{byte(i)})
			l.Attributes().PutInt("i", int64(i))
		}
	}

	direct, viaPdata := convertBothTraces(t, td)
	require.Equal(t, viaPdata, direct)

	spans := direct.Resources[0].Scopes[0].Spans
	require.Len(t, spans, 20)
	assert.Empty(t, spans[1].Events, "a span without events keeps none")
	assert.Empty(t, spans[1].Links, "a span without links keeps none")
}

// TestConvertTracesIsFieldOrderIndependent is the traces twin of the logs ordering test: the same
// message written ascending and descending must decode identically.
func TestConvertTracesIsFieldOrderIndependent(t *testing.T) {
	t.Parallel()

	var asc, desc otlpdirect.TracesConverter

	up, err := asc.Convert(encodeScopeSpans(true))
	require.NoError(t, err)

	down, err := desc.Convert(encodeScopeSpans(false))
	require.NoError(t, err)

	require.Equal(t, canonicalTraces(down), canonicalTraces(up))

	ss := up.Resources[0].Scopes[0]
	assert.Equal(t, "https://schema.example/scope", string(ss.Scope.SchemaURL))
	assert.Equal(t, "scope-name", string(ss.Scope.Name))
	assert.Equal(t, "https://schema.example/resource", string(up.Resources[0].Resource.SchemaURL))

	sp := ss.Spans[0]
	assert.Equal(t, "op", string(sp.Name))
	assert.Equal(t, "upstream timeout", string(sp.StatusMessage))
	assert.Len(t, sp.Events, 1)
	assert.Len(t, sp.Links, 1)
}

func TestConvertTracesEmpty(t *testing.T) {
	t.Parallel()

	var c otlpdirect.TracesConverter

	got, err := c.Convert(nil)
	require.NoError(t, err)
	assert.Empty(t, got.Resources)

	direct, viaPdata := convertBothTraces(t, ptrace.NewTraces())
	assert.Equal(t, viaPdata, direct)
}

func TestConvertTracesReuseIsIsolated(t *testing.T) {
	t.Parallel()

	var c otlpdirect.TracesConverter

	first := ptrace.NewTraces()
	fs := first.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	fs.SetName("first")
	fs.SetStartTimestamp(1)
	fs.Events().AppendEmpty().SetName("ev")
	fs.Attributes().PutStr("a", "1")

	second := ptrace.NewTraces()
	sspan := second.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sspan.SetName("second")
	sspan.SetStartTimestamp(2)

	_, err := c.Convert(marshalTraces(t, first))
	require.NoError(t, err)

	got, err := c.Convert(marshalTraces(t, second))
	require.NoError(t, err)

	want := &trace.Traces{}
	require.Zero(t, pdataconv.AppendTraces(want, second))
	assert.Equal(t, canonicalTraces(want), canonicalTraces(got))
}

func BenchmarkConvertTraces(b *testing.B) {
	td := ptrace.NewTraces()

	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("bench")

	for i := range 1_000 {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID([16]byte{byte(i), byte(i >> 8)})
		sp.SetSpanID([8]byte{byte(i)})
		sp.SetName("GET /api/v1/things")
		sp.SetKind(ptrace.SpanKindServer)
		sp.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + i))
		sp.SetEndTimestamp(pcommon.Timestamp(1_700_000_000_001_000_000 + i))
		sp.Status().SetCode(ptrace.StatusCodeOk)
		sp.Attributes().PutStr("http.method", "GET")
		sp.Attributes().PutInt("http.status_code", 200)
	}

	raw := marshalTraces(b, td)

	b.Run("Direct", func(b *testing.B) {
		var c otlpdirect.TracesConverter

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			if _, err := c.Convert(raw); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Pdata", func(b *testing.B) {
		var u ptrace.ProtoUnmarshaler

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			decoded, err := u.UnmarshalTraces(raw)
			if err != nil {
				b.Fatal(err)
			}

			dst := &trace.Traces{}
			pdataconv.AppendTraces(dst, decoded)
		}
	})
}
