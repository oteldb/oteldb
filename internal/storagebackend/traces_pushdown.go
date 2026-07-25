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
	// exact reports that the filter keeps a span if and only if the engine's own evaluater would:
	// it is not merely a superset. A stream-level matcher is never exact, since it decides whole
	// streams by resource *and* scope labels while the engine reads one of the two per span.
	exact bool
}

// tracePushdown selects the candidate traces of a TraceQL query: the union of its filter groups.
// Every group is a superset of the spans the engine would keep, so the traces it selects are a
// superset of the query's result — the engine still evaluates the full expression on them.
type tracePushdown struct {
	groups []traceFilter
	// pushed reports that the pushdown was built and run, i.e. that the candidate ids are
	// meaningful. A zero tracePushdown means the caller scans the whole window instead.
	pushed bool
	// exact reports that the candidate traces are exactly the traces holding a span the pushed
	// filters keep — every matcher was lowered, and every lowering is exact. It says nothing about
	// whether the matcher list is the whole query; that is
	// [traceqlengine.SelectSpansetsParams.Exact].
	exact bool
}

// buildTracePushdown lowers the span matchers extracted from a TraceQL expression to storage
// filters. It reports false when nothing can be pushed, in which case the caller scans the window.
//
// The op decides how a matcher that cannot be lowered is handled. For a conjunction it is simply
// left to the engine: the remaining filters only widen the candidate set. For a union (`&&`/`||`
// between spansets, a structural operator) every branch must lower, since dropping one would lose
// the traces only it selects. A union also needs one group per matcher: fetch conditions over
// distinct columns AND within a request, so an OR cannot be expressed as a single fetch.
func buildTracePushdown(op traceql.SpansetOp, matchers []traceql.SpanMatcher) (tracePushdown, bool) {
	if len(matchers) == 0 {
		return tracePushdown{}, false
	}

	if op != traceql.SpansetOpAnd {
		groups := make([]traceFilter, 0, len(matchers))
		for _, m := range matchers {
			f, ok := lowerSpanMatcher(m)
			if !ok {
				return tracePushdown{}, false
			}
			groups = append(groups, f)
		}
		return tracePushdown{groups: groups}, true
	}

	var (
		group traceFilter
		exact = true
	)
	for _, m := range matchers {
		f, ok := lowerSpanMatcher(m)
		if !ok || !f.exact {
			// A dropped matcher, or one lowered to a superset, widens the candidate set.
			exact = false
		}
		if !ok {
			continue
		}
		group.matchers = append(group.matchers, f.matchers...)
		group.conditions = append(group.conditions, f.conditions...)
	}
	if len(group.matchers)+len(group.conditions) == 0 {
		return tracePushdown{}, false
	}
	return tracePushdown{groups: []traceFilter{group}, exact: exact}, true
}

// lowerSpanMatcher lowers one span matcher to a storage filter, reporting false when its property,
// operator or value has no sound storage form.
func lowerSpanMatcher(m traceql.SpanMatcher) (traceFilter, bool) {
	switch {
	case m.Op == 0:
		// A bare attribute reference. It is extracted from `by(...)`/`select(...)` too, where the
		// attribute need not be present at all, so it is not a filter.
		return traceFilter{}, false
	case m.Attribute.Parent:
		// `parent.`-scoped attributes are not evaluated by the engine either.
		return traceFilter{}, false
	case m.Static.Type == traceql.TypeNil:
		// nil equals nil, so a missing attribute *matches*: the inverse of what a condition does.
		return traceFilter{}, false
	}

	pred, ok := staticPredicate(m.Op, m.Static)
	if !ok {
		return traceFilter{}, false
	}

	switch attr := m.Attribute; attr.Prop {
	case traceql.SpanDuration:
		return spanCondition(sigtrace.ColDuration, durationStatic, pred), true
	case traceql.SpanName:
		f := spanCondition(sigtrace.ColName, stringStatic, pred)
		// The name column carries a full-text bloom: an exact value's own tokens are present in
		// every part that holds it, so they prune parts that do not.
		if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
			f.conditions[0].Tokens = bloom.SafeTokens(nil, []byte(m.Static.AsString()), true, true)
		}
		return f, true
	case traceql.SpanStatus:
		return spanCondition(sigtrace.ColStatusCode, statusStatic, pred), true
	case traceql.SpanKind:
		return spanCondition(sigtrace.ColKind, kindStatic, pred), true
	case traceql.SpanStatusMessage:
		return spanCondition(sigtrace.ColStatusMsg, stringStatic, pred), true
	case traceql.SpanID:
		return spanCondition(sigtrace.ColSpanID, hexStatic, pred), true
	case traceql.ParentID:
		return spanCondition(sigtrace.ColParentSpanID, parentHexStatic, pred), true
	case traceql.TraceID:
		f := spanCondition(sigtrace.ColTraceID, hexStatic, pred)
		// trace_id carries an equality bloom: the trace-by-id lookup prunes to the parts holding it.
		if raw, ok := decodeTraceIDHex(m.Op, m.Static); ok {
			f.conditions[0].Equal = &fetch.EqualMatcher{Name: sigtrace.ColTraceID, Value: string(raw)}
		}
		return f, true
	case traceql.SpanAttribute:
		return lowerAttributeMatcher(m, pred)
	default:
		// Spanset-level intrinsics (rootName, rootServiceName, traceDuration), the nested-set
		// properties, childCount, parent, and the event/link properties have no per-span column form.
		return traceFilter{}, false
	}
}

// lowerAttributeMatcher lowers a scoped attribute matcher: a span attribute becomes a per-record
// condition over the serialized attrs column, a resource or instrumentation attribute a postings
// matcher that prunes whole streams.
//
// Only [absentSafeOp] operators are pushed. Both forms drop a span (or stream) that does not carry
// the attribute at all, while the engine evaluates a missing attribute to nil — so an operator that
// is true against nil would lose real matches.
//
// An unscoped attribute (`.name`) is not pushed: the engine resolves it against span, scope *and*
// resource attributes, and neither form alone covers all three.
func lowerAttributeMatcher(m traceql.SpanMatcher, pred func(traceql.Static) bool) (traceFilter, bool) {
	attr := m.Attribute
	if attr.Name == "" || !absentSafeOp(m.Op) {
		return traceFilter{}, false
	}

	// Exact string equality is the one shape whose bloom token is exactly the stored value's own
	// text, so it is the only one hinted; a typed (numeric, bool) value's text form need not match
	// the static's, and testing it could wrongly prune a part that holds a match.
	var equal *fetch.EqualMatcher
	if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
		equal = &fetch.EqualMatcher{Name: attr.Name, Value: m.Static.AsString()}
	}

	switch attr.Scope {
	case traceql.ScopeSpan:
		f := spanCondition(attr.Name, attrStatic, pred)
		f.conditions[0].Equal = equal
		return f, true
	case traceql.ScopeResource, traceql.ScopeInstrumentation:
		// Resource and scope attributes are both stream labels, so one matcher covers either scope.
		// That is exactly what `resource.` means to the engine (it reads scope attributes too), and a
		// superset for `instrumentation.` (which reads only scope attributes) — the engine re-checks.
		//
		// Not exact, therefore: it decides a whole stream on either label set, while the engine reads
		// one of the two (and prefers the scope's on a conflict), so a kept stream may hold no span
		// the engine keeps.
		return traceFilter{matchers: []fetch.Matcher{{
			Name:  []byte(attr.Name),
			Match: func(v signal.Value) bool { return pred(attrStatic(v)) },
			Spec:  equal,
		}}}, true
	default:
		return traceFilter{}, false
	}
}

// spanCondition builds a filter of one per-span condition over column, evaluating pred on the
// column value projected to a [traceql.Static] by conv.
// A per-span condition is exact: the column is always present and conv mirrors the engine's
// evaluater, so the condition keeps a span if and only if the engine would.
func spanCondition(column string, conv func(signal.Value) traceql.Static, pred func(traceql.Static) bool) traceFilter {
	return traceFilter{exact: true, conditions: []fetch.Condition{{
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

// absentSafeOp reports whether op evaluates to false against a missing attribute, and is therefore
// safe to push as a filter that drops the spans (or streams) lacking it.
//
// The engine evaluates a missing attribute to nil, and [traceql.Static.Compare] reports -1 for a nil
// against a typed static (incomparable), so `!=`, `<` and `<=` are *true* on a missing attribute:
// pushing them would drop spans the query matches. They stay in the engine.
func absentSafeOp(op traceql.BinaryOp) bool {
	switch op {
	case traceql.OpEq, traceql.OpGt, traceql.OpGte, traceql.OpRe, traceql.OpNotRe:
		return true
	default:
		return false
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
