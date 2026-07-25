package storagebackend

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/traceql"
)

// TestBuildTracePushdown pins which TraceQL shapes reach storage and as what: a condition over a
// per-span column (a fixed column or an attribute key), or a matcher over a stream label.
func TestBuildTracePushdown(t *testing.T) {
	tests := []struct {
		query string
		// groups is the expected lowering, one entry per candidate-trace fetch: the condition columns
		// and the matcher label names it carries. Nil ⇒ nothing is pushed.
		groups []pushdownGroup
	}{
		// Nothing to push: no matcher at all, or no per-span column form.
		{query: `{}`},
		{query: `{rootName = "GET /"}`},
		{query: `{rootServiceName = "frontend"}`},
		{query: `{traceDuration > 1s}`},
		{query: `{nestedSetLeft > 3}`},
		{query: `{childCount > 1}`},
		{query: `{event:name = "exception"}`},
		{query: `{link:spanID = "0011223344556677"}`},
		{query: `{instrumentation:name = "oteldb"}`},
		{query: `{parent.span.http.route = "/route/7"}`},
		// A bare attribute reference is extracted from by()/select() and the aggregates too, where the
		// attribute need not be present, so it never becomes a filter.
		{query: `{} | count() > 1`},
		// `= nil` matches a *missing* attribute (nil is equal to nil), the inverse of what a filter
		// that drops the spans lacking the column does.
		{query: `{span.http.route = nil}`},

		// The intrinsics with a column.
		{query: `{name = "checkout.process"}`, groups: []pushdownGroup{{conditions: []string{"name"}}}},
		{query: `{name != "checkout.process"}`, groups: []pushdownGroup{{conditions: []string{"name"}}}},
		{query: `{name =~ "checkout.*"}`, groups: []pushdownGroup{{conditions: []string{"name"}}}},
		{query: `{name =~ "checkout(.*"}`}, // an invalid pattern stays in the engine
		{query: `{status = error}`, groups: []pushdownGroup{{conditions: []string{"status_code"}}}},
		{query: `{kind = server}`, groups: []pushdownGroup{{conditions: []string{"kind"}}}},
		{query: `{duration > 150ms}`, groups: []pushdownGroup{{conditions: []string{"duration"}}}},
		{query: `{duration <= 150ms}`, groups: []pushdownGroup{{conditions: []string{"duration"}}}},
		{query: `{statusMessage = "declined"}`, groups: []pushdownGroup{{conditions: []string{"status_message"}}}},
		{query: `{span:id = "0011223344556677"}`, groups: []pushdownGroup{{conditions: []string{"span_id"}}}},
		{query: `{span:parentID = "0011223344556677"}`, groups: []pushdownGroup{{conditions: []string{"parent_span_id"}}}},
		{
			query:  `{trace:id = "00112233445566770011223344556677"}`,
			groups: []pushdownGroup{{conditions: []string{"trace_id"}}},
		},

		// Span attributes become conditions over the serialized attrs column, keyed by the raw
		// attribute name — but only for the operators that are false on a missing attribute.
		{query: `{span.http.route = "/route/7"}`, groups: []pushdownGroup{{conditions: []string{"http.route"}}}},
		{query: `{span.http.route =~ "/route/.*"}`, groups: []pushdownGroup{{conditions: []string{"http.route"}}}},
		{query: `{span.http.route !~ "/route/7"}`, groups: []pushdownGroup{{conditions: []string{"http.route"}}}},
		{query: `{span.status_code >= 500}`, groups: []pushdownGroup{{conditions: []string{"status_code"}}}},
		{query: `{span.http.route != "/route/7"}`},
		{query: `{span.http.status < 500}`},
		{query: `{span.http.status <= 500}`},
		// `!= nil` is true only for a present attribute, i.e. exactly an existence filter.
		{query: `{span.http.route != nil}`, groups: []pushdownGroup{{conditions: []string{"http.route"}}}},

		// An unscoped attribute is the union of the span-attribute and the stream-label form, since
		// the engine resolves it against span, scope and resource attributes at once.
		{
			query: `{.http.route = "/route/7"}`,
			groups: []pushdownGroup{
				{conditions: []string{"http.route"}},
				{matchers: []string{"http.route"}},
			},
		},
		{query: `{.http.route != "/route/7"}`},
		// The alternatives distribute over the rest of the conjunction, one fetch per combination.
		{
			query: `{.http.route = "/route/7" && status = error}`,
			groups: []pushdownGroup{
				{conditions: []string{"http.route", "status_code"}},
				{conditions: []string{"status_code"}, matchers: []string{"http.route"}},
			},
		},
		// Beyond maxTracePushdownGroups combinations the extra matcher is left to the engine rather
		// than fanning out into more fetches.
		{
			query: `{.a = "1" && .b = "2" && .c = "3"}`,
			groups: []pushdownGroup{
				{conditions: []string{"a", "b"}},
				{conditions: []string{"a"}, matchers: []string{"b"}},
				{conditions: []string{"b"}, matchers: []string{"a"}},
				{matchers: []string{"a", "b"}},
			},
		},

		// Resource and instrumentation attributes prune whole streams via the postings index.
		{query: `{resource.service.name = "payments"}`, groups: []pushdownGroup{{matchers: []string{"service.name"}}}},
		{query: `{resource.service.name =~ "pay.*"}`, groups: []pushdownGroup{{matchers: []string{"service.name"}}}},
		{query: `{instrumentation.library = "x"}`, groups: []pushdownGroup{{matchers: []string{"library"}}}},
		{query: `{resource.service.name != "payments"}`},

		// A conjunction is one fetch; a matcher it cannot lower is simply left to the engine.
		{
			query: `{resource.service.name = "payments" && span.http.route = "/route/7" && status = error}`,
			groups: []pushdownGroup{{
				conditions: []string{"http.route", "status_code"},
				matchers:   []string{"service.name"},
			}},
		},
		{
			query:  `{status = error && span.http.route != "/route/7"}`,
			groups: []pushdownGroup{{conditions: []string{"status_code"}}},
		},

		// A union needs one fetch per branch, and every branch must lower.
		{
			query: `{span.http.route = "/route/7"} && {status = error}`,
			groups: []pushdownGroup{
				{conditions: []string{"http.route"}},
				{conditions: []string{"status_code"}},
			},
		},
		{
			query: `{name = "a"} >> {resource.service.name = "payments"}`,
			groups: []pushdownGroup{
				{conditions: []string{"name"}},
				{matchers: []string{"service.name"}},
			},
		},
		{query: `{status = error || span.http.route != "/route/7"}`},
		{query: `{rootName = "GET /"} || {status = error}`},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)

			pd, ok := buildTracePushdown(traceql.ExtractMatchers(expr))
			require.Equal(t, len(tt.groups) > 0, ok, "pushed")
			if !ok {
				return
			}
			require.Equal(t, tt.groups, describePushdown(pd))
		})
	}
}

// pushdownGroup is the observable shape of one [traceFilter]: the columns its conditions filter and
// the label names its matchers select on.
type pushdownGroup struct {
	conditions []string
	matchers   []string
}

func describePushdown(pd tracePushdown) []pushdownGroup {
	out := make([]pushdownGroup, 0, len(pd.groups))
	for _, g := range pd.groups {
		var d pushdownGroup
		for _, c := range g.conditions {
			d.conditions = append(d.conditions, c.Column)
		}
		for _, m := range g.matchers {
			d.matchers = append(d.matchers, string(m.Name))
		}
		out = append(out, d)
	}
	return out
}

// TestTracePushdownHints asserts the bloom hints a lowered filter carries: the tokens of an exact
// name, the raw bytes of an exact trace id, and the value of an exact string attribute. A hint only
// ever prunes parts, so a missing one costs speed, but a wrong one loses results.
func TestTracePushdownHints(t *testing.T) {
	condition := func(t *testing.T, query string) fetch.Condition {
		t.Helper()

		expr, err := traceql.Parse(query)
		require.NoError(t, err)
		pd, ok := buildTracePushdown(traceql.ExtractMatchers(expr))
		require.True(t, ok)
		require.Len(t, pd.groups, 1)
		require.Len(t, pd.groups[0].conditions, 1)

		return pd.groups[0].conditions[0]
	}

	t.Run("NameTokens", func(t *testing.T) {
		c := condition(t, `{name = "checkout.process"}`)
		require.Equal(t, [][]byte{[]byte("checkout"), []byte("process")}, c.Tokens)
		require.Nil(t, c.Equal)
	})
	t.Run("NameRegexpNotHinted", func(t *testing.T) {
		require.Empty(t, condition(t, `{name =~ "checkout.*"}`).Tokens)
	})
	t.Run("TraceIDEqual", func(t *testing.T) {
		c := condition(t, `{trace:id = "0f0e0d0c0b0a09080706050403020100"}`)
		raw, err := hex.DecodeString("0f0e0d0c0b0a09080706050403020100")
		require.NoError(t, err)
		require.Equal(t, &fetch.EqualMatcher{Name: "trace_id", Value: string(raw)}, c.Equal)
	})
	t.Run("AttributeEqual", func(t *testing.T) {
		c := condition(t, `{span.http.route = "/route/7"}`)
		require.Equal(t, &fetch.EqualMatcher{Name: "http.route", Value: "/route/7"}, c.Equal)
	})
	t.Run("AttributeNonStringNotHinted", func(t *testing.T) {
		// A typed value's bloom token is its text form, which need not match the static's, so an
		// exact integer attribute carries no hint.
		require.Nil(t, condition(t, `{span.http.response.status_code = 500}`).Equal)
	})
}

// TestTraceStaticProjections covers the column→[traceql.Static] projections the pushed predicates
// evaluate on, which must agree with the engine's own evaluaters.
func TestTraceStaticProjections(t *testing.T) {
	var want traceql.Static

	want.SetString("db.query")
	require.Equal(t, want, stringStatic(signal.StringValue([]byte("db.query"))))

	want.SetDuration(150 * time.Millisecond)
	require.Equal(t, want, durationStatic(signal.IntValue(int64(150*time.Millisecond))))

	want.SetSpanStatus(ptrace.StatusCodeError)
	require.Equal(t, want, statusStatic(signal.IntValue(int64(ptrace.StatusCodeError))))

	want.SetSpanKind(ptrace.SpanKindServer)
	require.Equal(t, want, kindStatic(signal.IntValue(int64(ptrace.SpanKindServer))))

	want.SetString("0011223344556677")
	require.Equal(t, want, hexStatic(signal.StringValue([]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77})))
	require.Equal(t, want, parentHexStatic(signal.StringValue([]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77})))

	// A root span's empty parent id evaluates to nil, as it does in the engine.
	want.SetNil()
	require.Equal(t, want, parentHexStatic(signal.StringValue(nil)))

	// Attribute values follow Static.SetOTELValue; a value it cannot represent is nil.
	want.SetString("GET")
	require.Equal(t, want, attrStatic(signal.StringValue([]byte("GET"))))
	want.SetInt(500)
	require.Equal(t, want, attrStatic(signal.IntValue(500)))
	want.SetNumber(1.5)
	require.Equal(t, want, attrStatic(signal.DoubleValue(1.5)))
	want.SetBool(true)
	require.Equal(t, want, attrStatic(signal.BoolValue(true)))
	want.SetNil()
	require.Equal(t, want, attrStatic(signal.Value{}))
}
