package traceqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestEvaluater(t *testing.T) {
	var (
		testSpanID      = otelstorage.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
		testParentSpanID = otelstorage.SpanID{9, 10, 11, 12, 13, 14, 15, 16}
		testTraceID     = otelstorage.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		testLinkTraceID = otelstorage.TraceID{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		testLinkSpanID  = otelstorage.SpanID{17, 18, 19, 20, 21, 22, 23, 24}
	)

	attrs := pcommon.NewMap()
	attrs.PutStr("http.method", "POST")
	attrs.PutInt("http.status_code", 200)
	attrs.PutBool("truth", true)

	eventAttrs := pcommon.NewMap()
	eventAttrs.PutStr("custom", "eventValue")

	linkAttrs := pcommon.NewMap()
	linkAttrs.PutStr("custom", "linkValue")

	scopeAttrs := pcommon.NewMap()
	scopeAttrs.PutStr("custom", "scopeValue")

	span := tracestorage.Span{
		TraceID:       testTraceID,
		SpanID:        testSpanID,
		ParentSpanID:  testParentSpanID,
		Name:          "spanName",
		StatusMessage: "ok message",
		Start:         1700000001_000000000,
		End:           1700000003_000000000,
		Kind:          int32(ptrace.SpanKindServer),
		StatusCode:    int32(ptrace.StatusCodeOk),
		Attrs:         otelstorage.Attrs(attrs),
		ScopeName:     "myInstrumentation",
		ScopeVersion:  "v1.0.0",
		ScopeAttrs:    otelstorage.Attrs(scopeAttrs),
		Events: []tracestorage.Event{
			{
				// 1s after span start.
				Timestamp: 1700000002_000000000,
				Name:      "eventName",
				Attrs:     otelstorage.Attrs(eventAttrs),
			},
		},
		Links: []tracestorage.Link{
			{
				TraceID: testLinkTraceID,
				SpanID:  testLinkSpanID,
				Attrs:   otelstorage.Attrs(linkAttrs),
			},
		},
	}
	ectx := EvaluateCtx{
		Set: Spanset{
			RootSpanName:    "rootName",
			RootServiceName: "rootServiceName",
			TraceDuration:   time.Minute,
			Spans: []tracestorage.Span{
				span,
			},
		},
	}

	tests := []struct {
		input string
		want  bool
	}{
		// String attribute.
		{`{ name = "spanName" }`, true},
		{`{ name != "spanName" }`, false},
		{`{ rootName = "rootName" && rootServiceName = "rootServiceName" }`, true},
		{`{ .http.method = "POST" || .http.method = "GET" }`, true},
		{`{ resource.http.method = "POST" || .http.method = "GET" }`, false},
		// String static.
		{`{ "foo" = "foo" }`, true},
		{`{ "foo" =~ "^foo$" }`, true},
		{`{ "foo" != "bar" }`, true},
		{`{ "foo" !~ "^bar$" }`, true},
		// Integer attribute.
		{`{ .http.status_code = 200 }`, true},
		{`{ .http.status_code = 200.0 }`, true},
		{`{ .http.status_code / 100 = 2 }`, true},
		// Integer static.
		{`{ -(10) = -10 }`, true},
		{`{ 4 / 2 = 2 }`, true},
		{`{ 4 / 2 = 2.0 }`, true},
		{`{ 3 / 2 = 1.5 }`, true},
		{`{ 0 / 2 != 0 }`, true},
		{`{ 4 % 2 = 0 }`, true},
		{`{ 3 % 2 = 1 }`, true},
		{`{ 0 % 2 != 0 }`, true},
		{`{ 2+3*4+5 = 19 }`, true},
		{`{ (2+3)*4+5 = 25 }`, true},
		{`{ (2+3)*(4+5) = 45 }`, true},
		{`{ 2^3 * 2 = 16 }`, true},
		{`{ 2 ^ 3^2 = 512 }`, true},
		{`{ -(2 ^ 3^2) = -512 }`, true},
		// Number static.
		{`{ -(10) = -1e1 }`, true},
		{`{ -(10.1) = -10.1 }`, true},
		// Boolean attribute.
		{`{ .truth }`, true},
		{`{ span.truth }`, true},
		{`{ !.truth }`, false},
		{`{ .truth = true }`, true},
		{`{ .truth != false }`, true},
		{`{ .truth || false }`, true},
		// Boolean static.
		{`{ true }`, true},
		{`{ !true }`, false},
		{`{ true && true }`, true},
		{`{ false && true }`, false},
		{`{ true && false }`, false},
		{`{ false && false }`, false},
		{`{ true || true }`, true},
		{`{ true || false }`, true},
		{`{ false || true }`, true},
		{`{ false || false }`, false},
		// Nilable attribute: span has a parent.
		{`{ parent = nil }`, false},
		{`{ parent != nil }`, true},
		// Nilable static.
		{`{ nil = nil }`, true},
		{`{ nil != nil }`, false},
		// Duration attribute.
		// Duration is 2s.
		{`{ duration < 10s }`, true},
		{`{ duration <= 2s }`, true},
		{`{ duration >= 2s }`, true},
		{`{ duration > 10s }`, false},
		{`{ -duration < 0 }`, true},
		{`{ duration-duration = 0 }`, true},
		{`{ duration+duration = 4s }`, true},
		{`{ duration*2 = 4s }`, true},
		{`{ 2*duration = 4s }`, true},
		// Trace duration is 1m.
		{`{ traceDuration > 10s }`, true},
		{`{ traceDuration >= 1m }`, true},
		{`{ traceDuration <= 1m }`, true},
		{`{ traceDuration > 10h }`, false},
		{`{ -traceDuration < 0 }`, true},
		{`{ traceDuration+traceDuration = 2m }`, true},
		{`{ traceDuration*2 = 2m }`, true},
		// Duration static.
		{`{ 10s = 10s }`, true},
		{`{ 1m = 60s }`, true},
		{`{ -1m = -60s }`, true},
		{`{ -(1m) = -(60s) }`, true},
		{`{ 1m+1m = 120s }`, true},
		// Status attribute.
		{`{ status = ok }`, true},
		{`{ status = error }`, false},
		{`{ status = unset }`, false},
		{`{ status != error }`, true},
		// Kind attribute.
		{`{ kind = server }`, true},
		{`{ kind = client }`, false},
		{`{ kind = internal }`, false},
		{`{ kind != client }`, true},
		// StatusMessage intrinsic.
		{`{ statusMessage = "ok message" }`, true},
		{`{ statusMessage != "other" }`, true},
		{`{ span:statusMessage = "ok message" }`, true},
		// SpanID intrinsic.
		{fmt.Sprintf(`{ span:id = %q }`, testSpanID.Hex()), true},
		{fmt.Sprintf(`{ span:id != %q }`, otelstorage.SpanID{}.Hex()), true},
		// ParentID intrinsic.
		{fmt.Sprintf(`{ span:parentId = %q }`, testParentSpanID.Hex()), true},
		{fmt.Sprintf(`{ span:parentId != %q }`, testSpanID.Hex()), true},
		// TraceID intrinsic.
		{fmt.Sprintf(`{ trace:id = %q }`, testTraceID.Hex()), true},
		// Instrumentation intrinsics.
		{`{ instrumentation:name = "myInstrumentation" }`, true},
		{`{ instrumentation:version = "v1.0.0" }`, true},
		// Event intrinsics.
		{`{ event:name = "eventName" }`, true},
		{`{ event:name != "other" }`, true},
		{`{ event:timeSinceStart >= 1s }`, true},
		{`{ event:timeSinceStart < 2s }`, true},
		// Link intrinsics.
		{fmt.Sprintf(`{ link:traceId = %q }`, testLinkTraceID.Hex()), true},
		{fmt.Sprintf(`{ link:spanId = %q }`, testLinkSpanID.Hex()), true},
		// Scoped attribute selectors.
		{`{ event.custom = "eventValue" }`, true},
		{`{ event.custom != "other" }`, true},
		{`{ link.custom = "linkValue" }`, true},
		{`{ link.custom != "other" }`, true},
		{`{ instrumentation.custom = "scopeValue" }`, true},
		{`{ instrumentation.custom != "other" }`, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Query: \n%s", tt.input)
				}
			}()
			a := require.New(t)

			expr := parseSpansetFilter(t, tt.input)
			e, err := buildEvaluater(expr)
			a.NoError(err)

			got := e.Eval(span, ectx)
			a.Equal(traceql.TypeBool, got.Type)
			a.Equal(tt.want, got.AsBool())
		})
	}
}

func parseSpansetFilter(t require.TestingT, input string) traceql.FieldExpr {
	root, err := traceql.Parse(input)
	require.NoError(t, err)

	require.IsType(t, (*traceql.SpansetPipeline)(nil), root)
	pipeline := root.(*traceql.SpansetPipeline).Pipeline

	require.IsType(t, (*traceql.SpansetFilter)(nil), pipeline[0])
	filter := pipeline[0].(*traceql.SpansetFilter)

	return filter.Expr
}
