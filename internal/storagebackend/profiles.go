package storagebackend

import (
	"context"
	"regexp"
	"slices"
	"sort"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigprofile "github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
)

var _ profilestorage.Querier = (*ProfileQuerier)(nil)

// reservedProfileLabels are the stream-identity labels carrying the profile type (see
// signal/profile). They are folded into a stream's identity like a metric's __name__ and are not
// user labels, so they are hidden from LabelNames/LabelValues and never matched against directly.
var reservedProfileLabels = map[string]struct{}{
	string(sigprofile.LabelSampleType): {},
	string(sigprofile.LabelSampleUnit): {},
	string(sigprofile.LabelPeriodType): {},
	string(sigprofile.LabelPeriodUnit): {},
}

// ProfileTypes implements [profilestorage.Querier]. It enumerates the distinct profile types of the
// tenant's streams in the window, reading the type out of each series' reserved labels.
func (q *ProfileQuerier) ProfileTypes(ctx context.Context, opts profilestorage.ProfileTypesOptions) ([]profileql.ProfileType, error) {
	start, end := seriesWindow(opts.Start, opts.End)
	series, err := q.b.store.ProfileSeries(ctx, q.b.tenant, nil, start, end)
	if err != nil {
		return nil, errors.Wrap(err, "profile series")
	}

	seen := map[string]profileql.ProfileType{}
	for _, s := range series {
		pt := profileTypeOf(s)
		seen[pt.ID()] = pt
	}

	out := make([]profileql.ProfileType, 0, len(seen))
	for _, pt := range seen {
		out = append(out, pt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out, nil
}

// LabelNames implements [profilestorage.Querier]. It returns the distinct user-label names of the
// streams matching the selector, excluding the reserved profile-type labels.
func (q *ProfileQuerier) LabelNames(ctx context.Context, opts profilestorage.LabelNamesOptions) ([]string, error) {
	matchers, err := profileFetchMatchers(opts.Type, opts.Matchers)
	if err != nil {
		return nil, err
	}
	start, end := seriesWindow(opts.Start, opts.End)
	series, err := q.b.store.ProfileSeries(ctx, q.b.tenant, matchers, start, end)
	if err != nil {
		return nil, errors.Wrap(err, "profile series")
	}

	names := map[string]struct{}{}
	forEachUserLabel(series, func(name, _ string) {
		names[name] = struct{}{}
	})
	return sortedKeys(names), nil
}

// LabelValues implements [profilestorage.Querier]. It returns the distinct values the given label
// takes across the streams matching the selector.
func (q *ProfileQuerier) LabelValues(ctx context.Context, label string, opts profilestorage.LabelValuesOptions) ([]string, error) {
	matchers, err := profileFetchMatchers(opts.Type, opts.Matchers)
	if err != nil {
		return nil, err
	}
	start, end := seriesWindow(opts.Start, opts.End)
	series, err := q.b.store.ProfileSeries(ctx, q.b.tenant, matchers, start, end)
	if err != nil {
		return nil, errors.Wrap(err, "profile series")
	}

	values := map[string]struct{}{}
	forEachUserLabel(series, func(name, value string) {
		if name == label {
			values[value] = struct{}{}
		}
	})
	return sortedKeys(values), nil
}

// SelectMergeProfile implements [profilestorage.Querier]. It fetches every matching sample, resolves
// each sample's content-addressed stack to function frames, and merges them into a single
// flamegraph tree.
func (q *ProfileQuerier) SelectMergeProfile(ctx context.Context, params profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
	matchers, err := profileFetchMatchers(&params.Type, params.Matchers)
	if err != nil {
		return nil, err
	}

	resolver, err := q.b.store.ProfileResolver(ctx, q.b.tenant)
	if err != nil {
		return nil, errors.Wrap(err, "profile resolver")
	}

	lo, hi := fetchWindow(params.Start, params.End)
	req := fetch.Request{
		Tenant:     q.b.tenant,
		Signal:     signal.Profile,
		Start:      lo,
		End:        hi,
		Matchers:   matchers,
		Projection: []string{sigprofile.ColStackID, sigprofile.ColValue},
	}

	it, err := q.b.store.ProfileFetcher(q.b.tenant).Fetch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch profiles")
	}
	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain profiles")
	}

	root := &profilestorage.FlameNode{Name: "root"}
	for _, batch := range batches {
		stacks, ok := batch.Column(sigprofile.ColStackID)
		if !ok {
			continue
		}
		values, hasValues := batch.Column(sigprofile.ColValue)
		for i, stackID := range stacks.Bytes {
			var value int64 = 1
			if hasValues && i < len(values.Int64) {
				value = values.Int64[i]
			}
			frames := resolver.Resolve(stackID)
			mergeStack(root, frames, value)
		}
	}

	return &profilestorage.FlameTree{Root: root}, nil
}

// mergeStack folds one sample's leaf-first frames into the tree rooted at root, accumulating value
// into every node on the root→leaf path (Total) and into the leaf (Self).
func mergeStack(root *profilestorage.FlameNode, frames []sigprofile.Frame, value int64) {
	root.Total += value
	if len(frames) == 0 {
		root.Self += value
		return
	}

	node := root
	// frames are leaf-first; walk the call path outermost→leaf so the tree reads root→leaf.
	for _, frame := range slices.Backward(frames) {
		name := frame.Function
		child := childByName(node, name)
		if child == nil {
			child = &profilestorage.FlameNode{Name: name}
			node.Children = append(node.Children, child)
		}
		child.Total += value
		node = child
	}
	node.Self += value
}

// childByName returns the named child of node, or nil when absent.
func childByName(node *profilestorage.FlameNode, name string) *profilestorage.FlameNode {
	for _, c := range node.Children {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// profileTypeOf reconstructs a [profileql.ProfileType] from a stream's reserved type labels. The
// Pyroscope-style Name is not carried in storage, so it is derived from the sample type.
func profileTypeOf(s signal.Series) profileql.ProfileType {
	get := func(key []byte) string {
		if v, ok := s.Resource.Attributes.Get(key); ok {
			return valueText(v)
		}
		if v, ok := s.Scope.Attributes.Get(key); ok {
			return valueText(v)
		}
		return ""
	}
	pt := profileql.ProfileType{
		SampleType: get(sigprofile.LabelSampleType),
		SampleUnit: get(sigprofile.LabelSampleUnit),
		PeriodType: get(sigprofile.LabelPeriodType),
		PeriodUnit: get(sigprofile.LabelPeriodUnit),
	}
	pt.Name = pt.SampleType
	return pt
}

// forEachUserLabel calls fn for every non-reserved resource/scope attribute of every series.
func forEachUserLabel(series []signal.Series, fn func(name, value string)) {
	for _, s := range series {
		visit := func(attrs signal.Attributes) {
			for _, kv := range attrs {
				name := string(kv.Key)
				if _, reserved := reservedProfileLabels[name]; reserved {
					continue
				}
				fn(name, valueText(kv.Value))
			}
		}
		visit(s.Resource.Attributes)
		visit(s.Scope.Attributes)
	}
}

// profileFetchMatchers builds the fetch matchers selecting the streams of a profile-type plus the
// user label matchers. The profile type contributes equality matchers on the reserved labels.
func profileFetchMatchers(typ *profileql.ProfileType, matchers []profileql.LabelMatcher) ([]fetch.Matcher, error) {
	var out []fetch.Matcher
	if typ != nil {
		add := func(name []byte, value string) {
			if value == "" {
				return
			}
			out = append(out, equalMatcher(name, value))
		}
		add(sigprofile.LabelSampleType, typ.SampleType)
		add(sigprofile.LabelSampleUnit, typ.SampleUnit)
		add(sigprofile.LabelPeriodType, typ.PeriodType)
		add(sigprofile.LabelPeriodUnit, typ.PeriodUnit)
	}
	for _, m := range matchers {
		fm, err := profileLabelMatcher(m)
		if err != nil {
			return nil, err
		}
		out = append(out, fm)
	}
	return out, nil
}

// profileLabelMatcher compiles one ProfileQL label matcher into a fetch matcher.
func profileLabelMatcher(m profileql.LabelMatcher) (fetch.Matcher, error) {
	name := []byte(string(m.Label))
	switch m.Op {
	case profileql.OpEq:
		return equalMatcher(name, m.Value), nil
	case profileql.OpNotEq:
		value := m.Value
		return fetch.Matcher{Name: name, Match: func(v signal.Value) bool {
			return valueText(v) != value
		}}, nil
	case profileql.OpRe, profileql.OpNotRe:
		re := m.Re
		if re == nil {
			compiled, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				return fetch.Matcher{}, errors.Wrap(err, "compile matcher")
			}
			re = compiled
		}
		negate := m.Op == profileql.OpNotRe
		return fetch.Matcher{Name: name, Match: func(v signal.Value) bool {
			return re.MatchString(valueText(v)) != negate
		}}, nil
	default:
		return fetch.Matcher{}, errors.Errorf("unsupported matcher op %v", m.Op)
	}
}

// equalMatcher builds an exact-equality fetch matcher, also setting the serializable Spec so the
// cluster fan-out can push it to peers.
func equalMatcher(name []byte, value string) fetch.Matcher {
	return fetch.Matcher{
		Name:  name,
		Match: func(v signal.Value) bool { return valueText(v) == value },
		Spec:  &fetch.EqualMatcher{Name: string(name), Value: value},
	}
}

// sortedKeys returns the sorted keys of set.
func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
