package scarecrow

import (
	"regexp"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// narrowSelectors uses the validated join mapping before either producer starts fetching.
func (o *vectorBinop) narrowSelectors(many *Schema) {
	allManyMatched := !slices.Contains(o.pairOf, -1)
	allOneMatched := !slices.Contains(o.probed, false)
	if allManyMatched && allOneMatched {
		return
	}
	manySelector, oneSelector := binopSelector(o.many), binopSelector(o.one)
	if !slices.ContainsFunc(o.pairOf, func(ref int) bool { return ref >= 0 }) {
		manySelector.scanNone, oneSelector.scanNone = true, true
		return
	}
	for _, name := range o.matching.MatchingLabels {
		matched := make(map[string]struct{})
		for ref, ls := range many.Series {
			if o.pairOf[ref] >= 0 {
				matched[ls.Get(name)] = struct{}{}
			}
		}
		// Missing and empty labels cannot use the native positive-postings shortcut.
		// Other matching labels can still supply necessary (not exact tuple) filters.
		if _, empty := matched[""]; empty {
			continue
		}
		values := make([]string, 0, len(matched))
		for v := range matched {
			values = append(values, regexp.QuoteMeta(v))
		}
		slices.Sort(values)
		m, err := labels.NewMatcher(labels.MatchRegexp, name, strings.Join(values, "|"))
		if err != nil {
			// An oversized regexp must not turn a valid query into a planning error.
			continue
		}
		if !allManyMatched {
			if manySelector.scanMatchers == nil {
				manySelector.scanMatchers = slices.Clone(manySelector.matchers)
			}
			manySelector.scanMatchers = append(manySelector.scanMatchers, m)
		}
		if !allOneMatched {
			if oneSelector.scanMatchers == nil {
				oneSelector.scanMatchers = slices.Clone(oneSelector.matchers)
			}
			oneSelector.scanMatchers = append(oneSelector.scanMatchers, m)
		}
	}
}

func (o *vectorBinop) canNarrowSelectors() bool {
	// Per-label matchers are necessary tuple filters; the join retains exact matching.
	if !o.matching.On || len(o.matching.MatchingLabels) == 0 {
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
