package storagebackend

import (
	"context"
	"sort"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

var (
	_ logstorage.Querier  = (*LogQuerier)(nil)
	_ logqlengine.Querier = (*LogQuerier)(nil)
)

// Capabilities implements [logqlengine.Querier]. The storage backend does not push any pipeline
// filtering down, so it advertises no supported ops; the LogQL engine applies the whole pipeline
// (line filters, parsers, label filters) on top of the raw entry stream this backend returns.
func (q *LogQuerier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	return caps
}

// Query implements [logqlengine.Querier]. It returns a node that streams the entries of the
// streams matching selector; the engine wraps it to evaluate the rest of the pipeline.
func (q *LogQuerier) Query(_ context.Context, selector []logql.LabelMatcher) (logqlengine.PipelineNode, error) {
	return &logStreamNode{q: q, selector: selector}, nil
}

// logStreamNode is the base [logqlengine.PipelineNode] over the storage logs fetcher.
type logStreamNode struct {
	q        *LogQuerier
	selector []logql.LabelMatcher
	// conditions are offloaded columnar predicates (e.g. line filters as body conditions) pushed
	// into the fetch by [LogQLOptimizer]. They prune parts and drop records at the storage layer;
	// the engine still applies the full pipeline, so they only ever skip work.
	conditions []fetch.Condition
}

var _ logqlengine.PipelineNode = (*logStreamNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *logStreamNode) Traverse(cb logqlengine.NodeVisitor) error { return cb(n) }

// EvalPipeline implements [logqlengine.PipelineNode]. It fetches every record in the window, builds
// each record's label set, keeps the records whose set satisfies the stream selector, and returns
// them as entries ordered per params.Direction (and truncated to params.Limit).
func (n *logStreamNode) EvalPipeline(ctx context.Context, params logqlengine.EvalParams) (logqlengine.EntryIterator, error) {
	lo, hi := fetchWindow(params.Start, params.End)
	req := fetch.Request{
		Tenant: n.q.b.tenant,
		Signal: signal.Log,
		Start:  lo,
		End:    hi,
		// Offload equality on resource/scope (stream) labels to the postings index, so the storage
		// prunes non-matching streams before any record is materialized.
		Matchers: n.streamMatchers(ctx, lo, hi),
	}
	if len(n.conditions) > 0 {
		// Offloaded line filters: let the storage prune parts and drop non-matching records.
		req.Conditions = n.conditions
		req.AllConditions = true
	}

	it, err := n.q.b.store.LogFetcher(n.q.b.tenant).Fetch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch logs")
	}
	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, errors.Wrap(err, "drain logs")
	}

	var entries []logqlengine.Entry
	for _, batch := range batches {
		cols := newLogColumns(batch)
		for i := range batch.Timestamps {
			record := cols.record(batch, i)
			set := logqlabels.NewLabelSet()
			set.SetFromRecord(record)
			if !matchSelector(set, n.selector) {
				continue
			}
			entries = append(entries, logqlengine.Entry{
				Timestamp: record.Timestamp,
				Line:      record.Body,
				Set:       set,
			})
		}
	}

	sortEntries(entries, params.Direction)
	if params.Limit > 0 && len(entries) > params.Limit {
		entries = entries[:params.Limit]
	}
	return iterators.Slice(entries), nil
}

// streamMatchers resolves the selector's equality matchers on resource/scope (stream) labels to
// storage fetch matchers, so the postings index prunes non-matching streams before any record is
// materialized. The in-memory matchSelector still re-checks every record, so this only ever skips
// work.
//
// A matcher is offloaded only when its label is an equality (`=`) on a non-empty value whose
// normalized name maps unambiguously to a single raw resource/scope attribute key (discovered via
// LogSeries). Record-attribute labels (e.g. Loki-style streams) and labels whose normalized name is
// absent or ambiguous are left to matchSelector, since the postings index keys streams by their raw
// resource/scope attributes only.
func (n *logStreamNode) streamMatchers(ctx context.Context, lo, hi int64) []fetch.Matcher {
	wanted := map[string]string{}
	for _, m := range n.selector {
		if m.Op == logql.OpEq && m.Value != "" {
			wanted[string(m.Label)] = m.Value
		}
	}
	if len(wanted) == 0 {
		return nil
	}

	series, err := n.q.b.store.LogSeries(ctx, n.q.b.tenant, nil, lo, hi)
	if err != nil {
		return nil // best effort: fall back to in-memory filtering.
	}

	// Map each wanted normalized label to the set of raw stream-attribute keys that normalize to it.
	rawKeys := map[string]map[string]struct{}{}
	collect := func(key []byte) {
		label := otelstorage.KeyToLabel(string(key))
		if _, ok := wanted[label]; !ok {
			return
		}
		set := rawKeys[label]
		if set == nil {
			set = map[string]struct{}{}
			rawKeys[label] = set
		}
		set[string(key)] = struct{}{}
	}
	for _, s := range series {
		for i := range s.Resource.Attributes {
			collect(s.Resource.Attributes[i].Key)
		}
		for i := range s.Scope.Attributes {
			collect(s.Scope.Attributes[i].Key)
		}
	}

	var matchers []fetch.Matcher
	for label, value := range wanted {
		keys := rawKeys[label]
		if len(keys) != 1 {
			// Absent (record attribute or unknown) or ambiguous: leave it to matchSelector.
			continue
		}
		var rawKey string
		for k := range keys {
			rawKey = k
		}
		want := value
		matchers = append(matchers, fetch.Matcher{
			Name:  []byte(rawKey),
			Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == want },
			Spec:  &fetch.EqualMatcher{Name: rawKey, Value: want},
		})
	}
	return matchers
}

// sortEntries orders entries by timestamp ascending for forward queries and descending otherwise.
func sortEntries(entries []logqlengine.Entry, dir logqlengine.Direction) {
	sort.SliceStable(entries, func(i, j int) bool {
		if dir == logqlengine.DirectionBackward {
			return entries[i].Timestamp > entries[j].Timestamp
		}
		return entries[i].Timestamp < entries[j].Timestamp
	})
}

// logColumns caches the byte/int columns of a log batch for row materialization.
type logColumns struct {
	body         [][]byte
	severityText [][]byte
	traceID      [][]byte
	spanID       [][]byte
	attrs        [][]byte
	severity     []int64
	resource     otelstorage.Attrs
	scopeName    string
	scopeVersion string
	scopeAttrs   otelstorage.Attrs
}

// newLogColumns extracts the named columns and stream identity of a log batch once.
func newLogColumns(batch *fetch.Batch) logColumns {
	bytesCol := func(name string) [][]byte {
		if c, ok := batch.Column(name); ok {
			return c.Bytes
		}
		return nil
	}
	intCol := func(name string) []int64 {
		if c, ok := batch.Column(name); ok {
			return c.Int64
		}
		return nil
	}
	return logColumns{
		body:         bytesCol(siglog.ColBody),
		severityText: bytesCol(siglog.ColSeverityText),
		traceID:      bytesCol(siglog.ColTraceID),
		spanID:       bytesCol(siglog.ColSpanID),
		attrs:        bytesCol(siglog.ColAttrs),
		severity:     intCol(siglog.ColSeverity),
		resource:     otelAttrs(batch.Series.Resource.Attributes),
		scopeName:    string(batch.Series.Scope.Name),
		scopeVersion: string(batch.Series.Scope.Version),
		scopeAttrs:   otelAttrs(batch.Series.Scope.Attributes),
	}
}

// record materializes row i into a [logstorage.Record].
func (c logColumns) record(batch *fetch.Batch, i int) logstorage.Record {
	r := logstorage.Record{
		Timestamp:     otelstorage.Timestamp(batch.Timestamps[i]),
		ResourceAttrs: c.resource,
		ScopeName:     c.scopeName,
		ScopeVersion:  c.scopeVersion,
		ScopeAttrs:    c.scopeAttrs,
	}
	if i < len(c.body) {
		r.Body = string(c.body[i])
	}
	if i < len(c.severityText) {
		r.SeverityText = string(c.severityText[i])
	}
	if i < len(c.severity) {
		r.SeverityNumber = plog.SeverityNumber(c.severity[i])
	}
	if i < len(c.traceID) {
		r.TraceID = otelTraceID(c.traceID[i])
	}
	if i < len(c.spanID) {
		r.SpanID = otelSpanID(c.spanID[i])
	}
	if i < len(c.attrs) {
		if attrs, _, err := signal.DecodeAttributes(c.attrs[i]); err == nil {
			r.Attrs = otelAttrs(attrs)
		}
	}
	return r
}

// matchSelector reports whether the label set satisfies every selector matcher, treating an absent
// label as the empty string (Loki semantics).
func matchSelector(set logqlabels.LabelSet, matchers []logql.LabelMatcher) bool {
	for _, m := range matchers {
		value, _ := set.GetString(m.Label)
		if !matchLabel(m, value) {
			return false
		}
	}
	return true
}

// matchLabel evaluates one LogQL label matcher against a value.
func matchLabel(m logql.LabelMatcher, value string) bool {
	switch m.Op {
	case logql.OpEq:
		return value == m.Value
	case logql.OpNotEq:
		return value != m.Value
	case logql.OpRe:
		return m.Re != nil && m.Re.MatchString(value)
	case logql.OpNotRe:
		return m.Re != nil && !m.Re.MatchString(value)
	default:
		return false
	}
}

// LabelNames implements [logstorage.Querier]. It returns the distinct label names of the streams
// matching the options' selector.
func (q *LogQuerier) LabelNames(ctx context.Context, opts logstorage.LabelsOptions) ([]string, error) {
	names := map[string]struct{}{}
	if err := q.forEachLogStreamLabel(ctx, opts.Start, opts.End, opts.Query.Matchers, func(name, _ string) {
		names[name] = struct{}{}
	}); err != nil {
		return nil, err
	}
	out := sortedKeys(names)
	if opts.Limit > 0 && len(out) > opts.Limit {
		out = out[:opts.Limit]
	}
	return out, nil
}

// LabelValues implements [logstorage.Querier]. It returns the distinct values of labelName across
// the streams matching the options' selector.
func (q *LogQuerier) LabelValues(ctx context.Context, labelName string, opts logstorage.LabelsOptions) (iterators.Iterator[logstorage.Label], error) {
	values := map[string]struct{}{}
	if err := q.forEachLogStreamLabel(ctx, opts.Start, opts.End, opts.Query.Matchers, func(name, value string) {
		if name == labelName {
			values[value] = struct{}{}
		}
	}); err != nil {
		return nil, err
	}
	keys := sortedKeys(values)
	if opts.Limit > 0 && len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
	}
	labelsOut := make([]logstorage.Label, len(keys))
	for i, v := range keys {
		labelsOut[i] = logstorage.Label{Name: labelName, Value: v}
	}
	return iterators.Slice(labelsOut), nil
}

// Series implements [logstorage.Querier]. It returns the label sets of the streams matching any of
// the option selectors.
func (q *LogQuerier) Series(ctx context.Context, opts logstorage.SeriesOptions) (logstorage.Series, error) {
	selectors := opts.Selectors
	if len(selectors) == 0 {
		selectors = []logql.Selector{{}}
	}

	seen := map[string]map[string]string{}
	for _, sel := range selectors {
		streams, err := q.logStreams(ctx, opts.Start, opts.End, sel.Matchers)
		if err != nil {
			return nil, err
		}
		for _, set := range streams {
			m := set.AsMap()
			key := seriesKey(m)
			seen[key] = m
		}
	}

	out := make(logstorage.Series, 0, len(seen))
	for _, key := range sortedKeys(toSet(seen)) {
		out = append(out, seen[key])
	}
	return out, nil
}

// DetectedLabels implements [logstorage.Querier]. It returns the cardinality of each stream label.
func (q *LogQuerier) DetectedLabels(ctx context.Context, opts logstorage.LabelsOptions) ([]logstorage.DetectedLabel, error) {
	values := map[string]map[string]struct{}{}
	if err := q.forEachLogStreamLabel(ctx, opts.Start, opts.End, opts.Query.Matchers, func(name, value string) {
		set, ok := values[name]
		if !ok {
			set = map[string]struct{}{}
			values[name] = set
		}
		set[value] = struct{}{}
	}); err != nil {
		return nil, err
	}

	out := make([]logstorage.DetectedLabel, 0, len(values))
	for _, name := range sortedKeys(toSet(values)) {
		out = append(out, logstorage.DetectedLabel{Name: name, Cardinality: len(values[name])})
	}
	return out, nil
}

// DetectedFields implements [logstorage.Querier]. The storage backend does not parse record fields,
// so it reports the stream labels as string fields with their value cardinality.
func (q *LogQuerier) DetectedFields(ctx context.Context, opts logstorage.LabelsOptions) ([]logstorage.DetectedField, error) {
	values := map[string]map[string]struct{}{}
	if err := q.forEachLogStreamLabel(ctx, opts.Start, opts.End, opts.Query.Matchers, func(name, value string) {
		set, ok := values[name]
		if !ok {
			set = map[string]struct{}{}
			values[name] = set
		}
		set[value] = struct{}{}
	}); err != nil {
		return nil, err
	}

	out := make([]logstorage.DetectedField, 0, len(values))
	for _, name := range sortedKeys(toSet(values)) {
		out = append(out, logstorage.DetectedField{
			Name:        name,
			Type:        "string",
			Cardinality: uint64(len(values[name])),
		})
	}
	return out, nil
}

// logStreams returns the label sets of the streams matching the selector within [start, end].
func (q *LogQuerier) logStreams(ctx context.Context, start, end time.Time, matchers []logql.LabelMatcher) ([]logqlabels.LabelSet, error) {
	lo, hi := seriesWindow(start, end)
	series, err := q.b.store.LogSeries(ctx, q.b.tenant, nil, lo, hi)
	if err != nil {
		return nil, errors.Wrap(err, "log series")
	}

	var out []logqlabels.LabelSet
	for _, s := range series {
		set := logqlabels.NewLabelSet()
		set.SetAttrs(otelAttrs(s.Resource.Attributes), otelAttrs(s.Scope.Attributes))
		if !matchSelector(set, matchers) {
			continue
		}
		out = append(out, set)
	}
	return out, nil
}

// forEachLogStreamLabel calls fn for every (name, value) label pair of every matching stream.
func (q *LogQuerier) forEachLogStreamLabel(ctx context.Context, start, end time.Time, matchers []logql.LabelMatcher, fn func(name, value string)) error {
	streams, err := q.logStreams(ctx, start, end, matchers)
	if err != nil {
		return err
	}
	for _, set := range streams {
		for name, value := range set.AsMap() {
			fn(name, value)
		}
	}
	return nil
}

// seriesKey is a deterministic key for a label set, used to dedupe series.
func seriesKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf []byte
	for _, k := range keys {
		buf = append(buf, k...)
		buf = append(buf, 0)
		buf = append(buf, m[k]...)
		buf = append(buf, 0)
	}
	return string(buf)
}

// toSet returns a set of the keys of m.
func toSet[V any](m map[string]V) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}
