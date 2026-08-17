package clusterquery

import (
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// matchesAll reports whether s satisfies every matcher over the present value of its named label.
// A peer applies only the equality subset of a request, so its answer is a superset and this is
// what narrows it back to the request — the same positive semantics the engine's postings
// resolution applies.
func matchesAll(s signal.Series, matchers []fetch.Matcher) bool {
	for i := range matchers {
		v, ok := lookupLabel(s, matchers[i].Name)
		if !ok || !matchers[i].Match(v) {
			return false
		}
	}

	return true
}

// lookupLabel resolves a label value the way the engine indexes it for matching: the series' own
// attributes, then the resource and scope attributes, then the reserved scope name/version labels.
// That is what lets a matcher on a resource label (service.name, say) re-filter a record signal's
// superset correctly.
func lookupLabel(s signal.Series, name []byte) (signal.Value, bool) {
	if v, ok := s.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Resource.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Scope.Attributes.Get(name); ok {
		return v, true
	}

	switch string(name) {
	case signal.LabelScopeName:
		if len(s.Scope.Name) > 0 {
			return signal.StringValue(s.Scope.Name), true
		}
	case signal.LabelScopeVersion:
		if len(s.Scope.Version) > 0 {
			return signal.StringValue(s.Scope.Version), true
		}
	}

	return signal.Value{}, false
}
