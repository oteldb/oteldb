package scarecrow

import (
	"regexp"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// narrowSelectors uses the validated join mapping before either producer starts fetching.
func (o *vectorBinop) narrowSelectors(many, one *Schema) {
	allManyMatched := !slices.Contains(o.pairOf, -1)
	allOneMatched := !slices.Contains(o.probed, false)
	if allManyMatched && allOneMatched {
		return
	}
	name := o.matching.MatchingLabels[0]
	for _, schema := range []*Schema{many, one} {
		for _, ls := range schema.Series {
			// Empty and absent labels cannot use the native positive-postings shortcut.
			if ls.Get(name) == "" {
				return
			}
		}
	}
	matched := make(map[string]struct{})
	for ref, ls := range many.Series {
		if o.pairOf[ref] >= 0 {
			matched[ls.Get(name)] = struct{}{}
		}
	}
	manySelector, oneSelector := binopSelector(o.many), binopSelector(o.one)
	if len(matched) == 0 {
		manySelector.scanNone, oneSelector.scanNone = true, true
		return
	}
	values := make([]string, 0, len(matched))
	for v := range matched {
		values = append(values, regexp.QuoteMeta(v))
	}
	slices.Sort(values)
	m, err := labels.NewMatcher(labels.MatchRegexp, name, strings.Join(values, "|"))
	if err != nil {
		// An oversized regexp must not turn a valid query into a planning error.
		return
	}
	if !allManyMatched {
		manySelector.scanMatchers = append(slices.Clone(manySelector.matchers), m)
	}
	if !allOneMatched {
		oneSelector.scanMatchers = append(slices.Clone(oneSelector.matchers), m)
	}
}

func (o *vectorBinop) canNarrowSelectors() bool {
	// Independent regexps cannot represent general multi-label tuple membership.
	if !o.matching.On || len(o.matching.MatchingLabels) != 1 {
		return false
	}
	lhs, rhs := binopSelector(o.lhs), binopSelector(o.rhs)
	if lhs == nil || rhs == nil {
		return false
	}
	// Identical selectors cannot benefit from narrowing; preserve their early prefetch.
	sameMatchers := slices.EqualFunc(lhs.matchers, rhs.matchers, func(a, b *labels.Matcher) bool {
		return a.Type == b.Type && a.Name == b.Name && a.Value == b.Value
	})
	return !sameMatchers || !slices.Equal(lhs.refs, rhs.refs)
}

// Restrict only direct selectors: a subtree can change labels or depend on unmatched series.
func binopSelector(op Operator) *vectorSelect {
	if c, ok := op.(*concurrent); ok {
		op = c.input
	}
	s, ok := op.(*vectorSelect)
	if !ok || s.emitTimestamp {
		return nil
	}
	return s
}
