package storagebackend

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// TestTracePushdownMatchesEngine is the differential test between the two evaluations of a TraceQL
// matcher: the engine's, over a materialized span, and the pushdown's, over the storage value of the
// column that span was built from.
//
// The pushdown is allowed to be a *superset* — it selects candidate traces the engine then filters —
// but it must never drop a span the engine keeps. That direction is the whole safety property: a
// pushed filter that under-selects silently loses results, with no error and no way for the engine to
// recover them. So every probe asserts `engine keeps ⇒ pushdown keeps`, and reports the cases where
// the pushdown is strictly wider (fine, just slower).
//
// The probes deliberately include the values where the two evaluations are most likely to part ways:
// a missing attribute (the engine evaluates it to nil, storage never calls the condition at all — the
// asymmetry [absentSafeOp] exists to handle), a value of the wrong type for the operator, and a root
// span's empty parent id.
func TestTracePushdownMatchesEngine(t *testing.T) {
	for _, tt := range tracePushdownDiffCases {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)

			pd, pushed := buildTracePushdown(traceql.ExtractMatchers(expr))
			if !pushed {
				// An engine-only shape: nothing reaches storage, so there is nothing to disagree.
				// These cases are still listed, with their probes, so that widening what gets pushed
				// (say, adding an operator to absentSafeOp) turns them into real differential cases
				// instead of silently pushing an unsound filter. See the `!=` and `<` cases below.
				require.True(t, tt.engineOnly, "case is not pushed but is not marked engineOnly")
				return
			}
			require.False(t, tt.engineOnly, "case is marked engineOnly but was pushed")

			var kept, rejected int
			for _, probe := range tt.probes {
				engineKeeps := traceDiffEngineKeeps(t, tt.query, probe)
				if engineKeeps {
					kept++
				} else {
					rejected++
				}

				t.Run(probe.name, func(t *testing.T) {
					pushedKeeps := traceDiffPushdownKeeps(t, pd, probe)
					if engineKeeps && !pushedKeeps {
						t.Fatalf("pushdown drops a span the engine keeps: %s over %s", tt.query, probe.name)
					}
					if pushedKeeps && !engineKeeps {
						t.Logf("pushdown is wider than the engine here (sound, just less selective)")
					}
				})
			}

			// `engine keeps ⇒ pushdown keeps` is vacuously true for a case the engine rejects
			// outright, so a case that never matches would pass while proving nothing. Require every
			// case to discriminate: at least one probe kept, at least one rejected.
			require.NotZero(t, kept, "no probe matches: the implication is vacuous")
			require.NotZero(t, rejected, "no probe is rejected: the case does not discriminate")
		})
	}
}

// traceDiffProbe is one span value, expressed twice: as the span the engine evaluates, and as the
// storage value the pushed filter evaluates.
type traceDiffProbe struct {
	name string
	// span sets the probed value on an otherwise fixed span.
	span func(*tracestorage.Span)
	// value is the same value as the storage column (or attribute) holds it.
	value signal.Value
	// absent marks a probe whose column or attribute carries no value at all. Storage drops such a
	// row without ever calling the condition, so the pushdown always rejects it — which is only sound
	// when the engine rejects it too.
	absent bool
	// label marks a probe evaluated against a stream matcher (a resource or scope label) rather than
	// a per-span condition.
	label bool
}

type traceDiffCase struct {
	query string
	// engineOnly marks a shape the pushdown deliberately refuses. Its probes are the ones that would
	// break if it were ever pushed, so the case fails the moment the refusal is relaxed.
	engineOnly bool
	probes     []traceDiffProbe
}

// traceDiffCases covers every property [lowerSpanMatcher] can push, with the operators it pushes.
var tracePushdownDiffCases = []traceDiffCase{
	{
		query: `{name = "checkout.process"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanName("checkout.process"), value: strValue("checkout.process")},
			{name: "other", span: spanName("cart.load"), value: strValue("cart.load")},
			{name: "empty", span: spanName(""), value: strValue("")},
		},
	},
	{
		query: `{name != "checkout.process"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanName("cart.load"), value: strValue("cart.load")},
			{name: "other", span: spanName("checkout.process"), value: strValue("checkout.process")},
			{name: "empty", span: spanName(""), value: strValue("")},
		},
	},
	{
		query: `{name =~ "checkout.*"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanName("checkout.process"), value: strValue("checkout.process")},
			{name: "other", span: spanName("cart.load"), value: strValue("cart.load")},
		},
	},
	{
		query: `{duration > 150ms}`,
		probes: []traceDiffProbe{
			{name: "above", span: spanDuration(200 * time.Millisecond), value: signal.IntValue(int64(200 * time.Millisecond))},
			{name: "equal", span: spanDuration(150 * time.Millisecond), value: signal.IntValue(int64(150 * time.Millisecond))},
			{name: "below", span: spanDuration(10 * time.Millisecond), value: signal.IntValue(int64(10 * time.Millisecond))},
			{name: "zero", span: spanDuration(0), value: signal.IntValue(0)},
		},
	},
	{
		query: `{duration <= 150ms}`,
		probes: []traceDiffProbe{
			{name: "below", span: spanDuration(10 * time.Millisecond), value: signal.IntValue(int64(10 * time.Millisecond))},
			{name: "equal", span: spanDuration(150 * time.Millisecond), value: signal.IntValue(int64(150 * time.Millisecond))},
			{name: "above", span: spanDuration(200 * time.Millisecond), value: signal.IntValue(int64(200 * time.Millisecond))},
		},
	},
	{
		query: `{status = error}`,
		probes: []traceDiffProbe{
			{name: "error", span: spanStatus(ptrace.StatusCodeError), value: signal.IntValue(int64(ptrace.StatusCodeError))},
			{name: "ok", span: spanStatus(ptrace.StatusCodeOk), value: signal.IntValue(int64(ptrace.StatusCodeOk))},
			{name: "unset", span: spanStatus(ptrace.StatusCodeUnset), value: signal.IntValue(int64(ptrace.StatusCodeUnset))},
		},
	},
	{
		query: `{kind = server}`,
		probes: []traceDiffProbe{
			{name: "server", span: spanKind(ptrace.SpanKindServer), value: signal.IntValue(int64(ptrace.SpanKindServer))},
			{name: "client", span: spanKind(ptrace.SpanKindClient), value: signal.IntValue(int64(ptrace.SpanKindClient))},
			{name: "unspecified", span: spanKind(ptrace.SpanKindUnspecified), value: signal.IntValue(int64(ptrace.SpanKindUnspecified))},
		},
	},
	{
		query: `{statusMessage = "declined"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanStatusMessage("declined"), value: strValue("declined")},
			{name: "other", span: spanStatusMessage("ok"), value: strValue("ok")},
			{name: "empty", span: spanStatusMessage(""), value: strValue("")},
		},
	},
	{
		query: `{span:id = "0011223344556677"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanID("0011223344556677"), value: hexValue("0011223344556677")},
			{name: "other", span: spanID("7766554433221100"), value: hexValue("7766554433221100")},
		},
	},
	{
		// The root-span case: the engine evaluates a missing parent to nil, and the column is empty.
		query: `{span:parentID = "0011223344556677"}`,
		probes: []traceDiffProbe{
			{name: "match", span: parentSpanID("0011223344556677"), value: hexValue("0011223344556677")},
			{name: "other", span: parentSpanID("7766554433221100"), value: hexValue("7766554433221100")},
			{name: "root", span: parentSpanID(""), value: strValue("")},
		},
	},
	{
		query: `{trace:id = "00112233445566770011223344556677"}`,
		probes: []traceDiffProbe{
			{name: "match", span: traceIDHex("00112233445566770011223344556677"), value: hexValue("00112233445566770011223344556677")},
			{name: "other", span: traceIDHex("77665544332211007766554433221100"), value: hexValue("77665544332211007766554433221100")},
		},
	},

	// Span attributes. The absent probe is the point: the engine sees nil, storage sees no row.
	{
		query: `{span.http.route = "/route/7"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanAttrStr("http.route", "/route/7"), value: strValue("/route/7")},
			{name: "other", span: spanAttrStr("http.route", "/route/8"), value: strValue("/route/8")},
			{name: "wrong_type", span: spanAttrInt("http.route", 7), value: signal.IntValue(7)},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		query: `{span.http.route =~ "/route/.*"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanAttrStr("http.route", "/route/7"), value: strValue("/route/7")},
			{name: "other", span: spanAttrStr("http.route", "/other"), value: strValue("/other")},
			{name: "wrong_type", span: spanAttrInt("http.route", 7), value: signal.IntValue(7)},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		// `!~` is pushed: it is false against a missing attribute, like `=`.
		query: `{span.http.route !~ "/route/7"}`,
		probes: []traceDiffProbe{
			{name: "match", span: spanAttrStr("http.route", "/other"), value: strValue("/other")},
			{name: "other", span: spanAttrStr("http.route", "/route/7"), value: strValue("/route/7")},
			{name: "wrong_type", span: spanAttrInt("http.route", 7), value: signal.IntValue(7)},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		query: `{span.http.status_code >= 500}`,
		probes: []traceDiffProbe{
			{name: "above", span: spanAttrInt("http.status_code", 503), value: signal.IntValue(503)},
			{name: "equal", span: spanAttrInt("http.status_code", 500), value: signal.IntValue(500)},
			{name: "below", span: spanAttrInt("http.status_code", 200), value: signal.IntValue(200)},
			{name: "double", span: spanAttrDouble("http.status_code", 503), value: signal.DoubleValue(503)},
			{name: "wrong_type", span: spanAttrStr("http.status_code", "500"), value: strValue("500")},
			{name: "absent", span: noAttr, absent: true},
		},
	},

	// The operators that are *true* against a missing attribute, and so must never be pushed: the
	// engine evaluates the absent attribute to nil and keeps the span, while any storage filter over
	// the attribute column drops the row before the engine sees it. Each carries the absent probe
	// that catches exactly that, so relaxing [absentSafeOp] fails here rather than in production.
	{
		query:      `{span.http.route != "/route/7"}`,
		engineOnly: true,
		probes: []traceDiffProbe{
			{name: "match", span: spanAttrStr("http.route", "/other"), value: strValue("/other")},
			{name: "other", span: spanAttrStr("http.route", "/route/7"), value: strValue("/route/7")},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		query:      `{span.http.status_code < 500}`,
		engineOnly: true,
		probes: []traceDiffProbe{
			{name: "below", span: spanAttrInt("http.status_code", 200), value: signal.IntValue(200)},
			{name: "above", span: spanAttrInt("http.status_code", 503), value: signal.IntValue(503)},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		query:      `{span.http.status_code <= 500}`,
		engineOnly: true,
		probes: []traceDiffProbe{
			{name: "equal", span: spanAttrInt("http.status_code", 500), value: signal.IntValue(500)},
			{name: "above", span: spanAttrInt("http.status_code", 503), value: signal.IntValue(503)},
			{name: "absent", span: noAttr, absent: true},
		},
	},
	{
		query:      `{resource.service.name != "payments"}`,
		engineOnly: true,
		probes: []traceDiffProbe{
			{name: "match", span: resourceAttrStr("cart"), value: strValue("cart"), label: true},
			{name: "other", span: resourceAttrStr("payments"), value: strValue("payments"), label: true},
			{name: "absent", span: noAttr, absent: true, label: true},
		},
	},

	// Resource attributes are stream labels, so the pushed form is a matcher, not a condition.
	{
		query: `{resource.service.name = "payments"}`,
		probes: []traceDiffProbe{
			{name: "match", span: resourceAttrStr("payments"), value: strValue("payments"), label: true},
			{name: "other", span: resourceAttrStr("cart"), value: strValue("cart"), label: true},
			{name: "absent", span: noAttr, absent: true, label: true},
		},
	},
	{
		query: `{resource.service.name =~ "pay.*"}`,
		probes: []traceDiffProbe{
			{name: "match", span: resourceAttrStr("payments"), value: strValue("payments"), label: true},
			{name: "other", span: resourceAttrStr("cart"), value: strValue("cart"), label: true},
			{name: "absent", span: noAttr, absent: true, label: true},
		},
	},
}

// traceDiffEngineKeeps reports whether the TraceQL engine keeps the probe's span, by running the real
// engine over a one-span trace. Using the engine itself (not a re-implementation of it) is what makes
// this a differential test rather than a second opinion.
func traceDiffEngineKeeps(t *testing.T, query string, probe traceDiffProbe) bool {
	t.Helper()

	var querier traceqlengine.MemoryQuerier
	querier.Add(traceDiffSpan(probe))

	engine := traceqlengine.NewEngine(&querier, traceqlengine.Options{})
	res, err := engine.Eval(context.Background(), query, traceqlengine.EvalParams{
		Start: t0.Add(-time.Hour),
		End:   t0.Add(time.Hour),
		Limit: 10,
	})
	require.NoError(t, err)

	return len(res.Traces) > 0
}

// traceDiffPushdownKeeps reports whether the lowered filter keeps the probe's storage value.
//
// An absent probe never reaches the filter: a per-span condition is evaluated against a column value
// that does not exist, and a stream matcher against a label the stream does not carry — storage drops
// both. Modeling that as "rejected" is exactly what makes the absent probes meaningful.
func traceDiffPushdownKeeps(t *testing.T, pd tracePushdown, probe traceDiffProbe) bool {
	t.Helper()

	if probe.absent {
		return false
	}

	groups := pushdownGroups(pd)
	require.Len(t, groups, 1, "a differential case must lower to exactly one filter")

	group := groups[0]
	if probe.label {
		require.Len(t, group.matchers, 1)
		return group.matchers[0].Match(probe.value)
	}
	require.Len(t, group.conditions, 1)
	return group.conditions[0].Match(probe.value)
}

// t0 is the fixed span time; a constant keeps the engine's window check out of the comparison.
var t0 = time.Unix(1_600_000_000, 0).UTC()

// traceDiffSpan builds the fixed span the probe mutates: a root span of its own trace, inside the
// window, with every probed field at a value no case matches by accident.
func traceDiffSpan(probe traceDiffProbe) tracestorage.Span {
	span := tracestorage.Span{
		TraceID:       otelstorage.TraceID{0x99},
		SpanID:        otelstorage.SpanID{0x99},
		Name:          "unset",
		Kind:          int32(ptrace.SpanKindInternal),
		Start:         otelstorage.NewTimestampFromTime(t0),
		End:           otelstorage.NewTimestampFromTime(t0.Add(time.Millisecond)),
		StatusCode:    int32(ptrace.StatusCodeUnset),
		StatusMessage: "unset",
	}
	probe.span(&span)
	return span
}

// The probe builders. Each sets one field of the span the engine evaluates, mirroring the storage
// value the same probe carries.

func spanName(v string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.Name = v }
}

func spanDuration(d time.Duration) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.End = otelstorage.NewTimestampFromTime(s.Start.AsTime().Add(d)) }
}

func spanStatus(code ptrace.StatusCode) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.StatusCode = int32(code) }
}

func spanKind(kind ptrace.SpanKind) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.Kind = int32(kind) }
}

func spanStatusMessage(v string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.StatusMessage = v }
}

func spanID(hexID string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) {
		raw, err := hex.DecodeString(hexID)
		if err != nil {
			panic(err)
		}
		s.SpanID = otelstorage.SpanID(raw)
	}
}

func parentSpanID(hexID string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) {
		if hexID == "" {
			s.ParentSpanID = otelstorage.SpanID{}
			return
		}
		raw, err := hex.DecodeString(hexID)
		if err != nil {
			panic(err)
		}
		s.ParentSpanID = otelstorage.SpanID(raw)
	}
}

func traceIDHex(hexID string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) {
		raw, err := hex.DecodeString(hexID)
		if err != nil {
			panic(err)
		}
		s.TraceID = otelstorage.TraceID(raw)
	}
}

func spanAttrStr(key, value string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.Attrs = attrsOf(func(m pcommon.Map) { m.PutStr(key, value) }) }
}

func spanAttrInt(key string, value int64) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.Attrs = attrsOf(func(m pcommon.Map) { m.PutInt(key, value) }) }
}

func spanAttrDouble(key string, value float64) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) { s.Attrs = attrsOf(func(m pcommon.Map) { m.PutDouble(key, value) }) }
}

// resourceAttrStr sets the resource label the stream-matcher cases select on.
func resourceAttrStr(value string) func(*tracestorage.Span) {
	return func(s *tracestorage.Span) {
		s.ResourceAttrs = attrsOf(func(m pcommon.Map) { m.PutStr("service.name", value) })
	}
}

// noAttr leaves the span without the probed attribute at all.
func noAttr(*tracestorage.Span) {}

func attrsOf(fill func(pcommon.Map)) otelstorage.Attrs {
	m := pcommon.NewMap()
	fill(m)
	return otelstorage.Attrs(m)
}

func strValue(s string) signal.Value { return signal.StringValue([]byte(s)) }

// hexValue is the storage form of an id column: the raw bytes the hex literal decodes to.
func hexValue(hexID string) signal.Value {
	raw, err := hex.DecodeString(hexID)
	if err != nil {
		panic(err)
	}
	return signal.StringValue(raw)
}
