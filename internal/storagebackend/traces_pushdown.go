package storagebackend

import (
	"encoding/hex"
	"regexp"
	"slices"
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

// tracePushdown selects the candidate traces of a TraceQL query: the intersection of its terms,
// each of which is the union of its filter groups. One group is one fetch.
//
// Two levels are needed because the predicates live at two levels. Span-level predicates of a
// conjunction share a group, so they must hold of the *same* span, as TraceQL requires. A
// spanset-level one (rootName, rootServiceName) constrains a different span than its neighbors do,
// so it gets its own term and the trace id sets intersect instead.
//
// Every term is a superset of the traces the engine would keep, so their intersection is too — the
// engine still evaluates the full expression on the result.
type tracePushdown struct {
	terms []traceTerm
}

// traceTerm is one candidate-trace constraint: a trace satisfies it when it matches any of the
// groups (they union, since conditions over distinct columns AND within a single fetch).
type traceTerm struct {
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
// the traces only it selects. A union also needs one group per branch: fetch conditions over
// distinct columns AND within a request, so an OR cannot be expressed as a single fetch.
//
// A conjunction builds one term of span-level filters plus one term per spanset-level (root)
// matcher, which the caller intersects. One matcher may itself lower to several alternatives (an
// unscoped attribute is a span attribute *or* a stream label); within the span-level term those
// distribute over the other filters, so its groups are the cross product. The total group count is
// bounded by [maxTracePushdownGroups].
func buildTracePushdown(op traceql.SpansetOp, matchers []traceql.SpanMatcher) (tracePushdown, bool) {
	if len(matchers) == 0 {
		return tracePushdown{}, false
	}

	if op != traceql.SpansetOpAnd {
		// Every branch, span-level or root-level, is just another union member here.
		var groups []traceFilter
		for _, m := range matchers {
			alts, ok := lowerSpanMatcher(m)
			if !ok {
				alts, ok = lowerRootMatcher(m)
			}
			if !ok || len(groups)+len(alts) > maxTracePushdownGroups {
				return tracePushdown{}, false
			}
			groups = append(groups, alts...)
		}
		return tracePushdown{terms: []traceTerm{{groups: groups}}}, true
	}

	var (
		terms []traceTerm
		// spanGroups stays nil until a span-level matcher lowers, so an all-root conjunction adds no
		// span term at all. rootFetches counts the groups the root terms already claim.
		spanGroups  []traceFilter
		rootFetches int
	)
	for _, m := range matchers {
		if alts, ok := lowerRootMatcher(m); ok {
			if rootFetches+len(alts)+len(spanGroups) > maxTracePushdownGroups {
				continue
			}
			rootFetches += len(alts)
			terms = append(terms, traceTerm{groups: alts})
			continue
		}

		alts, ok := lowerSpanMatcher(m)
		if !ok {
			continue
		}
		base := spanGroups
		if base == nil {
			base = []traceFilter{{}}
		}
		if rootFetches+len(base)*len(alts) > maxTracePushdownGroups {
			continue
		}
		spanGroups = distribute(base, alts)
	}
	if spanGroups != nil {
		terms = append(terms, traceTerm{groups: spanGroups})
	}
	if len(terms) == 0 {
		return tracePushdown{}, false
	}
	return tracePushdown{terms: terms}, true
}

// distribute ANDs every group with every alternative, i.e. the cross product of a conjunction with a
// matcher's disjunction.
func distribute(groups, alts []traceFilter) []traceFilter {
	out := make([]traceFilter, 0, len(groups)*len(alts))
	for _, g := range groups {
		for _, alt := range alts {
			combined := traceFilter{
				matchers:   slices.Concat(g.matchers, alt.matchers),
				conditions: slices.Concat(g.conditions, alt.conditions),
			}
			out = append(out, combined)
		}
	}
	return out
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
		// traceDuration (an aggregate over the trace's spans), the nested-set properties, childCount,
		// parent, and the event/link properties have no per-span column form. rootName and
		// rootServiceName do, but not at this level — see [lowerRootMatcher].
		return nil, false
	}
}

// lowerRootMatcher lowers a spanset-level root intrinsic: `rootName` to a condition on the name of
// a parentless span, `rootServiceName` to the same root condition plus a service.name stream
// matcher (the engine reads the root's own resource attribute).
//
// It constrains a different span than its neighbors in a conjunction do, so the caller keeps it in
// its own term and intersects trace ids rather than ANDing conditions within one fetch.
//
// # Rootless traces
//
// A trace whose root span was never ingested (or starts outside the query window — the span fetch is
// windowed) has no parentless span, and the engine reports its root name and service as empty. This
// filter cannot select such a trace, so a predicate that accepts the empty string is not pushed at
// all; every other predicate is false on it in the engine too, and dropping it is exact. A trace
// with several parentless spans is covered as a superset: the term matches on any of them, and the
// engine re-checks against the first one, which is the root it uses.
func lowerRootMatcher(m traceql.SpanMatcher) ([]traceFilter, bool) {
	if m.Op == 0 || m.Attribute.Parent {
		return nil, false
	}
	pred, ok := staticPredicate(m.Op, m.Static)
	if !ok {
		return nil, false
	}

	// A root span is one with no parent span id.
	isRoot := fetch.Condition{
		Column: sigtrace.ColParentSpanID,
		Match:  func(v signal.Value) bool { return len(v.Str()) == 0 },
	}

	// A rootless trace has an empty root name and service, so a predicate that accepts the empty
	// string matches traces this filter — which selects parentless spans — cannot see at all.
	if matchesEmptyRoot(pred) {
		return nil, false
	}

	switch m.Attribute.Prop {
	case traceql.RootSpanName:
		name := spanCondition(sigtrace.ColName, stringStatic, pred)
		if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
			name.conditions[0].Tokens = bloom.SafeTokens(nil, []byte(m.Static.AsString()), true, true)
		}
		return []traceFilter{{conditions: append([]fetch.Condition{isRoot}, name.conditions...)}}, true
	case traceql.RootServiceName:
		// A root without a service.name also reports the empty service name, which a stream matcher
		// (it selects streams that *have* the label) cannot express — already refused above.
		if !falseOnMissing(pred) {
			return nil, false
		}

		var spec *fetch.EqualMatcher
		if m.Op == traceql.OpEq && m.Static.Type == traceql.TypeString {
			spec = &fetch.EqualMatcher{Name: serviceNameKey, Value: m.Static.AsString()}
		}
		return []traceFilter{{
			conditions: []fetch.Condition{isRoot},
			matchers: []fetch.Matcher{{
				Name:  []byte(serviceNameKey),
				Match: func(v signal.Value) bool { return pred(attrStatic(v)) },
				Spec:  spec,
			}},
		}}, true
	default:
		return nil, false
	}
}

// serviceNameKey is the resource attribute [tracestorage.Span.ServiceName] reads, i.e. the one
// backing the engine's rootServiceName.
const serviceNameKey = "service.name"

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

// matchesEmptyRoot reports whether the predicate accepts the empty string, which is what the engine
// reports as the root name and service of a trace with no parentless span.
func matchesEmptyRoot(pred func(traceql.Static) bool) bool {
	var empty traceql.Static
	empty.SetString("")

	return pred(empty)
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
