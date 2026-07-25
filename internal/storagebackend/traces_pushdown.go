package storagebackend

import (
	"encoding/hex"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/traceql"
)

// traceFilter is one group of storage-level filters: every stream matcher and every per-span
// condition in it must hold (they AND) for a span to be a candidate.
type traceFilter struct {
	matchers   []fetch.Matcher
	conditions []fetch.Condition
}

// tracePushdown selects the candidate traces of a TraceQL query: the union of its filter groups.
// Every group is a superset of the spans the engine would keep, so the traces it selects are a
// superset of the query's result — the engine still evaluates the full expression on them.
type tracePushdown struct {
	groups []traceFilter
}

// maxTracePushdownGroups bounds the groups a lowering may produce, since each one is its own fetch.
// A matcher whose alternatives would exceed it is left to the engine (in a conjunction) or gives up
// the pushdown (in a union), rather than trading a scan for several.
const maxTracePushdownGroups = 4

// buildTracePushdown lowers the span matchers extracted from a TraceQL expression to storage
// filters. It reports false when nothing can be pushed, in which case the caller scans the window.
//
// The op decides how a matcher that cannot be lowered is handled. For a conjunction it is simply
// left to the engine: the remaining filters only widen the candidate set. For a union (`&&`/`||`
// between spansets, a structural operator) every branch must lower, since dropping one would lose
// the traces only it selects. A union also needs one group per matcher: fetch conditions over
// distinct columns AND within a request, so an OR cannot be expressed as a single fetch.
//
// One matcher may itself lower to several alternatives (an unscoped attribute is a span attribute
// *or* a stream label). In a conjunction those distribute over the other filters, so the groups are
// the cross product — bounded by [maxTracePushdownGroups].
func buildTracePushdown(op traceql.SpansetOp, matchers []traceql.SpanMatcher) (tracePushdown, bool) {
	if len(matchers) == 0 {
		return tracePushdown{}, false
	}

	if op != traceql.SpansetOpAnd {
		var groups []traceFilter
		for _, m := range matchers {
			alts, ok := lowerSpanMatcher(m)
			if !ok || len(groups)+len(alts) > maxTracePushdownGroups {
				return tracePushdown{}, false
			}
			groups = append(groups, alts...)
		}
		return tracePushdown{groups: groups}, true
	}

	var (
		groups = []traceFilter{{}}
		pushed bool
	)
	for _, m := range matchers {
		alts, ok := lowerSpanMatcher(m)
		if !ok || len(groups)*len(alts) > maxTracePushdownGroups {
			continue
		}
		groups = distribute(groups, alts)
		pushed = true
	}
	if !pushed {
		return tracePushdown{}, false
	}
	return tracePushdown{groups: groups}, true
}

// distribute ANDs every group with every alternative, i.e. the cross product of a conjunction with a
// matcher's disjunction.
func distribute(groups, alts []traceFilter) []traceFilter {
	out := make([]traceFilter, 0, len(groups)*len(alts))
	for _, g := range groups {
		for _, alt := range alts {
			combined := traceFilter{
				matchers:   concat(g.matchers, alt.matchers),
				conditions: concat(g.conditions, alt.conditions),
			}
			out = append(out, combined)
		}
	}
	return out
}

// concat returns a fresh slice of a followed by b, so distributing never aliases a group's backing
// array into its siblings.
func concat[T any](a, b []T) []T {
	if len(a)+len(b) == 0 {
		return nil
	}
	out := make([]T, 0, len(a)+len(b))
	out = append(out, a...)

	return append(out, b...)
}

// lowerSpanMatcher lowers one span matcher to the alternatives whose union covers it (usually one),
// reporting false when its property, operator or value has no sound storage form.
func lowerSpanMatcher(m traceql.SpanMatcher) ([]traceFilter, bool) {
	switch {
	case m.Op == 0:
		// A bare attribute reference. It is extracted from `by(...)`/`select(...)` too, where the
		// attribute need not be present at all, so it is not a filter.
		return nil, false
	case m.Attribute.Parent:
		// `parent.`-scoped attributes are not evaluated by the engine either.
		return nil, false
	}

	pred, ok := staticPredicate(m.Op, m.Static)
	if !ok {
		return nil, false
	}

	one := func(f traceFilter) ([]traceFilter, bool) { return []traceFilter{f}, true }

	switch attr := m.Attribute; attr.Prop {
	case traceql.SpanDuration:
		return one(spanCondition(sigtrace.ColDuration, durationStatic, pred))
	case traceql.SpanName:
		f := spanCondition(sigtrace.ColName, stringStatic, pred)
		// The name column carries a full-text bloom: an exact value's own tokens are present in
		// every part that holds it, so they prune parts that do not.
		if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
			f.conditions[0].Tokens = bloom.SafeTokens(nil, []byte(m.Static.AsString()), true, true)
		}
		return one(f)
	case traceql.SpanStatus:
		return one(spanCondition(sigtrace.ColStatusCode, statusStatic, pred))
	case traceql.SpanKind:
		return one(spanCondition(sigtrace.ColKind, kindStatic, pred))
	case traceql.SpanStatusMessage:
		return one(spanCondition(sigtrace.ColStatusMsg, stringStatic, pred))
	case traceql.SpanID:
		return one(spanCondition(sigtrace.ColSpanID, hexStatic, pred))
	case traceql.ParentID:
		return one(spanCondition(sigtrace.ColParentSpanID, parentHexStatic, pred))
	case traceql.TraceID:
		f := spanCondition(sigtrace.ColTraceID, hexStatic, pred)
		// trace_id carries an equality bloom: the trace-by-id lookup prunes to the parts holding it.
		if raw, ok := decodeTraceIDHex(m.Op, m.Static); ok {
			f.conditions[0].Equal = &fetch.EqualMatcher{Name: sigtrace.ColTraceID, Value: string(raw)}
		}
		return one(f)
	case traceql.SpanAttribute:
		return lowerAttributeMatcher(m, pred)
	default:
		// Spanset-level intrinsics (rootName, rootServiceName, traceDuration), the nested-set
		// properties, childCount, parent, and the event/link properties have no per-span column form.
		return nil, false
	}
}

// lowerAttributeMatcher lowers a scoped attribute matcher. A span attribute becomes a per-record
// condition over the serialized attrs column; a resource or instrumentation attribute a postings
// matcher over stream labels; an unscoped attribute (`.name`) both, as alternatives, since the
// engine resolves it against the span, scope and resource attributes at once.
//
// Every form drops a span (or stream) that does not carry the attribute at all, while the engine
// evaluates a missing attribute to nil. So a predicate is only pushed when it is false on nil —
// which is asked of the predicate itself rather than derived from the operator, so `!= nil` (true
// only for a present attribute, i.e. exactly an existence filter) is pushed, while `= nil`, `!=`,
// `<` and `<=` (all true on a missing attribute) stay in the engine.
func lowerAttributeMatcher(m traceql.SpanMatcher, pred func(traceql.Static) bool) ([]traceFilter, bool) {
	attr := m.Attribute
	if attr.Name == "" || !falseOnMissing(pred) {
		return nil, false
	}

	// Exact string equality is the one shape whose bloom token is exactly the stored value's own
	// text, so it is the only one hinted; a typed (numeric, bool) value's text form need not match
	// the static's, and testing it could wrongly prune a part that holds a match.
	var equal *fetch.EqualMatcher
	if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
		equal = &fetch.EqualMatcher{Name: attr.Name, Value: m.Static.AsString()}
	}

	// Resource and scope attributes are both stream labels, so one matcher covers either scope. That
	// is exactly what `resource.` means to the engine (it reads scope attributes too), and a superset
	// for `instrumentation.` (which reads only scope attributes) — the engine re-checks either way.
	streamFilter := func() traceFilter {
		return traceFilter{matchers: []fetch.Matcher{{
			Name:  []byte(attr.Name),
			Match: func(v signal.Value) bool { return pred(attrStatic(v)) },
			Spec:  equal,
		}}}
	}
	spanFilter := func() traceFilter {
		f := spanCondition(attr.Name, attrStatic, pred)
		f.conditions[0].Equal = equal
		return f
	}

	switch attr.Scope {
	case traceql.ScopeSpan:
		return []traceFilter{spanFilter()}, true
	case traceql.ScopeResource, traceql.ScopeInstrumentation:
		return []traceFilter{streamFilter()}, true
	case traceql.ScopeNone:
		return []traceFilter{spanFilter(), streamFilter()}, true
	default:
		// Event and link attributes live inside the serialized events/links blobs.
		return nil, false
	}
}

// falseOnMissing reports whether the predicate rejects a missing attribute, which the engine
// evaluates to nil. Only such a predicate may be pushed as a filter that drops the spans (or
// streams) lacking the attribute outright.
func falseOnMissing(pred func(traceql.Static) bool) bool {
	var missing traceql.Static
	missing.SetNil()

	return !pred(missing)
}

// spanCondition builds a filter of one per-span condition over column, evaluating pred on the
// column value projected to a [traceql.Static] by conv.
func spanCondition(column string, conv func(signal.Value) traceql.Static, pred func(traceql.Static) bool) traceFilter {
	return traceFilter{conditions: []fetch.Condition{{
		Column: column,
		Match:  func(v signal.Value) bool { return pred(conv(v)) },
	}}}
}

// staticPredicate builds the predicate `value <op> want` with the comparison semantics the TraceQL
// engine itself applies (buildBinaryOp in traceqlengine), so a pushed predicate can never disagree
// with the engine on a value the storage produced. It reports false for an operator or pattern the
// engine does not compare with either.
func staticPredicate(op traceql.BinaryOp, want traceql.Static) (func(traceql.Static) bool, bool) {
	cmp := func(keep func(int) bool) func(traceql.Static) bool {
		return func(v traceql.Static) bool { return keep(v.Compare(want)) }
	}
	switch op {
	case traceql.OpEq:
		return cmp(func(c int) bool { return c == 0 }), true
	case traceql.OpNotEq:
		return cmp(func(c int) bool { return c != 0 }), true
	case traceql.OpGt:
		return cmp(func(c int) bool { return c > 0 }), true
	case traceql.OpGte:
		return cmp(func(c int) bool { return c >= 0 }), true
	case traceql.OpLt:
		return cmp(func(c int) bool { return c < 0 }), true
	case traceql.OpLte:
		return cmp(func(c int) bool { return c <= 0 }), true
	case traceql.OpRe, traceql.OpNotRe:
		if want.Type != traceql.TypeString {
			return nil, false
		}
		re, err := regexp.Compile(want.AsString())
		if err != nil {
			return nil, false
		}
		match := op == traceql.OpRe
		return func(v traceql.Static) bool {
			// A non-string value matches neither the pattern nor its negation, as in the engine.
			return v.Type == traceql.TypeString && re.MatchString(v.AsString()) == match
		}, true
	default:
		return nil, false
	}
}

// decodeTraceIDHex decodes an exact `trace:id` equality's hex literal to the raw column bytes.
func decodeTraceIDHex(op traceql.BinaryOp, want traceql.Static) ([]byte, bool) {
	if op != traceql.OpEq || want.Type != traceql.TypeString {
		return nil, false
	}
	raw, err := hex.DecodeString(want.AsString())
	if err != nil {
		return nil, false
	}
	return raw, true
}

// The column projections. Each mirrors the [traceqlengine] evaluater of the property it serves, so
// the pushed predicate sees the same [traceql.Static] the engine would build for that span.

func stringStatic(v signal.Value) (r traceql.Static) {
	r.SetString(string(v.Str()))
	return r
}

func durationStatic(v signal.Value) (r traceql.Static) {
	r.SetDuration(time.Duration(v.Int()))
	return r
}

func statusStatic(v signal.Value) (r traceql.Static) {
	r.SetSpanStatus(ptrace.StatusCode(v.Int()))
	return r
}

func kindStatic(v signal.Value) (r traceql.Static) {
	r.SetSpanKind(ptrace.SpanKind(v.Int()))
	return r
}

func hexStatic(v signal.Value) (r traceql.Static) {
	r.SetString(hex.EncodeToString(v.Str()))
	return r
}

// parentHexStatic projects a parent span id, which the engine evaluates to nil for a root span.
func parentHexStatic(v signal.Value) (r traceql.Static) {
	if raw := v.Str(); len(raw) > 0 {
		r.SetString(hex.EncodeToString(raw))
	} else {
		r.SetNil()
	}
	return r
}

// attrStatic projects a stored attribute value, mirroring [traceql.Static.SetOTELValue]: a value it
// cannot represent (a slice, a map, unset) evaluates to nil, exactly as the engine's attribute
// evaluater does.
func attrStatic(v signal.Value) (r traceql.Static) {
	switch v.Kind() {
	case signal.KindStr:
		r.SetString(string(v.Str()))
	case signal.KindBytes:
		r.SetString(string(v.Bytes()))
	case signal.KindInt:
		r.SetInt(v.Int())
	case signal.KindDouble:
		r.SetNumber(v.Double())
	case signal.KindBool:
		r.SetBool(v.Bool())
	default:
		r.SetNil()
	}
	return r
}
