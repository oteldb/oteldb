package storagebackend

import (
	"cmp"
	"context"
	"slices"
	"sort"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/storage"
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
	// pipelineLabels are equality label filters (`| L="v"`) extracted from the pipeline by
	// [LogQLOptimizer] that can be resolved against the stored label set, just like selector
	// matchers. streamFilters resolves them to Matchers/Conditions.
	pipelineLabels []logql.LabelMatcher
}

var _ logqlengine.PipelineNode = (*logStreamNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *logStreamNode) Traverse(cb logqlengine.NodeVisitor) error { return cb(n) }

// EvalPipeline implements [logqlengine.PipelineNode]. It fetches every record in the window, builds
// each record's label set, keeps the records whose set satisfies the stream selector, and returns
// them as entries ordered per params.Direction (and truncated to params.Limit).
func (n *logStreamNode) EvalPipeline(ctx context.Context, params logqlengine.EvalParams) (logqlengine.EntryIterator, error) {
	lo, hi := fetchWindow(params.Start, params.End)
	// Offload equality matchers: resource/scope labels prune streams via the postings index, clean
	// record-attribute labels drop records via a per-record condition — both before materialization.
	matchers, selConds := n.streamFilters(ctx, lo, hi)
	req := fetch.Request{
		Tenant:   n.q.b.tenant,
		Signal:   signal.Log,
		Start:    lo,
		End:      hi,
		Matchers: matchers,
	}
	// Selector record-attribute conditions plus any offloaded line filters (set by LogQLOptimizer).
	if len(selConds)+len(n.conditions) > 0 {
		conds := make([]fetch.Condition, 0, len(selConds)+len(n.conditions))
		conds = append(conds, selConds...)
		conds = append(conds, n.conditions...)
		req.Conditions = conds
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

	entries, err := n.materialize(ctx, batches)
	if err != nil {
		return nil, err
	}

	sortEntries(entries, params.Direction)
	if params.Limit > 0 && len(entries) > params.Limit {
		entries = entries[:params.Limit]
	}
	return iterators.Slice(entries), nil
}

// logMaterializeThreshold is the minimum number of fetched records before the parallel materializer
// is used; below it the sequential path wins (goroutine + merge overhead). It is a var so tests can
// exercise the parallel path on small inputs.
var logMaterializeThreshold = 2048

// materialize builds the matching entries from the fetched batches. With [Backend] log parallelism
// enabled and enough records it splits the flattened record sequence into contiguous chunks built
// concurrently and merges them in order; otherwise it runs sequentially. Because the chunks are
// contiguous and merged in order, the merged entries are in the same order as the sequential path,
// so the later stable sort yields the same output regardless of worker scheduling.
func (n *logStreamNode) materialize(ctx context.Context, batches []*fetch.Batch) ([]logqlengine.Entry, error) {
	offsets := make([]int, len(batches)+1)
	total := 0
	for b, batch := range batches {
		offsets[b] = total
		total += len(batch.Timestamps)
	}
	offsets[len(batches)] = total

	workers := n.q.b.logParallelism
	if workers <= 1 || total < logMaterializeThreshold {
		return n.materializeRange(batches, offsets, 0, total), nil
	}
	if workers > total {
		workers = total
	}

	chunkSize := (total + workers - 1) / workers
	results := make([][]logqlengine.Entry, workers)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(workers)
	for w := range workers {
		lo := w * chunkSize
		hi := min(lo+chunkSize, total)
		if lo >= hi {
			break
		}
		grp.Go(func() error {
			if err := grpCtx.Err(); err != nil {
				return err
			}
			results[w] = n.materializeRange(batches, offsets, lo, hi)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	merged := make([]logqlengine.Entry, 0, total)
	for _, r := range results {
		merged = append(merged, r...)
	}
	return merged, nil
}

// materializeRange materializes the matching entries for the global record range [lo, hi) over the
// flattened batch sequence (offsets[b] is the global index of batch b's first record). It is the
// single materialization primitive shared by the sequential and parallel paths, so both produce the
// same entries in the same order for a given range. It reads only immutable batch state and builds a
// fresh label set per record, so it is safe to call concurrently over disjoint ranges.
func (n *logStreamNode) materializeRange(batches []*fetch.Batch, offsets []int, lo, hi int) []logqlengine.Entry {
	out := make([]logqlengine.Entry, 0, max(hi-lo, 0))
	// Per-call scratch (safe: each concurrent range owns its own). A record dropped by the selector
	// reuses the scratch label set and attribute map; a kept record hands them off into the entry and
	// the scratch is replaced, since the entry's label set aliases the attribute map's values.
	var attrBuf signal.Attributes
	set := logqlabels.NewLabelSet()
	attrMap := pcommon.NewMap()
	for b := range batches {
		bLo, bHi := offsets[b], offsets[b+1]
		if bHi <= lo || bLo >= hi {
			continue
		}
		batch := batches[b]
		cols := newLogColumns(batch)
		start := max(lo-bLo, 0)
		end := min(hi-bLo, len(batch.Timestamps))
		for i := start; i < end; i++ {
			set.Reset()
			record := cols.recordInto(batch, i, &attrBuf, attrMap)
			set.SetFromRecord(record)
			if !matchSelector(set, n.selector) {
				attrMap.Clear() // reuse the map for the next dropped row
				continue
			}
			out = append(out, logqlengine.Entry{
				Timestamp: record.Timestamp,
				Line:      record.Body,
				Set:       set,
			})
			// The entry retains set, which aliases attrMap's values, so neither can be reused.
			set = logqlabels.NewLabelSet()
			attrMap = pcommon.NewMap()
		}
	}
	return out
}

// streamFilters resolves the selector's equality matchers to storage fetch filters so the storage
// drops data before any record is materialized into an entry with a label set (the expensive part of
// the scan). The in-memory matchSelector still re-checks every surviving record, so this only ever
// skips work.
//
// Resolution uses the engine's key index (LogKeys), which reports every attribute key in the window
// together with the scope(s) it was observed in (resource/scope = stream identity, record = the
// per-record attrs column). For an equality (`=`) on a non-empty value, the normalized label is
// matched back to its raw key(s):
//   - a single raw key seen only in stream scope becomes a postings Matcher that prunes whole streams;
//   - a single raw key seen only in record scope becomes a per-record Condition that drops
//     non-matching records — this resolves dotted labels too (http_method ← http.method).
//
// Absent labels, collisions (a label mapping to multiple raw keys), and mixed-scope keys (resource on
// some streams, record on others — soundly pushable as neither) are left to matchSelector.
// Record-attribute Conditions carry only an exact Match (no bloom Equal hint): the value may be
// non-string, and a typed value bloom token could wrongly part-prune; the Match still drops rows at
// the fetch layer.
func (n *logStreamNode) streamFilters(ctx context.Context, lo, hi int64) (matchers []fetch.Matcher, conditions []fetch.Condition) {
	wanted := map[string]string{}
	addEq := func(matchers []logql.LabelMatcher) {
		for _, m := range matchers {
			if m.Op == logql.OpEq && m.Value != "" {
				wanted[string(m.Label)] = m.Value
			}
		}
	}
	addEq(n.selector)       // stream selector labels
	addEq(n.pipelineLabels) // offloaded pipeline label filters (| L="v")
	if len(wanted) == 0 {
		return nil, nil
	}

	keys, err := n.q.b.store.LogKeys(ctx, n.q.b.tenant, lo, hi)
	if err != nil {
		return nil, nil // best effort: fall back to in-memory filtering.
	}

	// Resolve each wanted normalized label to its raw attribute key(s) and their merged scope. A
	// label maps to multiple raw keys only on a collision (e.g. http.method and http_method both
	// present); the scope bitset distinguishes stream identity from per-record attributes.
	type resolved struct {
		rawKey string
		scope  storage.KeyScope
	}
	byLabel := map[string][]resolved{}
	for _, ki := range keys {
		label := otelstorage.KeyToLabel(string(ki.Key))
		if _, ok := wanted[label]; !ok {
			continue
		}
		byLabel[label] = append(byLabel[label], resolved{rawKey: string(ki.Key), scope: ki.Scope})
	}

	for label, value := range wanted {
		cands := byLabel[label]
		if len(cands) != 1 {
			// Absent (label not stored) or ambiguous (collision): leave it to matchSelector.
			continue
		}
		c, want := cands[0], value
		stream := c.scope&(storage.KeyScopeResource|storage.KeyScopeScope) != 0
		record := c.scope&storage.KeyScopeRecord != 0
		switch {
		case stream && !record:
			// A resource/scope label: prune whole streams via the postings index.
			matchers = append(matchers, fetch.Matcher{
				Name: []byte(c.rawKey),
				// Render the value exactly as the label set does, so this matches matchSelector.
				Match: func(v signal.Value) bool { return labelValueString(v) == want },
				Spec:  &fetch.EqualMatcher{Name: c.rawKey, Value: want},
			})
		case record && !stream:
			// A per-record attribute: drop non-matching records at the fetch layer. Match only (no
			// bloom Equal hint): a typed value token could wrongly part-prune.
			conditions = append(conditions, fetch.Condition{
				Column: c.rawKey,
				Match:  func(v signal.Value) bool { return labelValueString(v) == want },
			})
		}
		// Mixed scope (resource on some streams, record on others) can't be pushed soundly as either
		// a Matcher or a Condition: leave it to matchSelector.
	}
	return matchers, conditions
}

// sortEntries orders entries by timestamp ascending for forward queries and descending otherwise.
// Uses slices.SortStableFunc (typed) rather than sort.SliceStable to avoid the reflection-based
// swapper, which dominates the sort of a large entry slice.
func sortEntries(entries []logqlengine.Entry, dir logqlengine.Direction) {
	slices.SortStableFunc(entries, func(a, b logqlengine.Entry) int {
		if dir == logqlengine.DirectionBackward {
			return cmp.Compare(b.Timestamp, a.Timestamp)
		}
		return cmp.Compare(a.Timestamp, b.Timestamp)
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

// recordInto materializes row i into a [logstorage.Record], decoding the record attributes into the
// reusable buffer attrBuf (avoiding a per-row slice allocation) and projecting them into the
// caller-owned attrMap (avoiding a per-row map allocation). The caller must pass an empty attrMap and
// is responsible for reusing or replacing it after the record's lifetime: a kept record's label set
// aliases attrMap's values, so it must be replaced; a dropped record's attrMap can be Clear()ed and
// reused. attrBuf may be nil for a one-shot decode.
func (c logColumns) recordInto(batch *fetch.Batch, i int, attrBuf *signal.Attributes, attrMap pcommon.Map) logstorage.Record {
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
		var (
			buf signal.Attributes
			err error
		)
		if attrBuf != nil {
			buf, _, err = signal.AppendAttributes((*attrBuf)[:0], c.attrs[i])
			*attrBuf = buf
		} else {
			buf, _, err = signal.DecodeAttributes(c.attrs[i])
		}
		if err == nil {
			fillOtelAttrs(attrMap, buf)
			r.Attrs = otelstorage.Attrs(attrMap)
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
	// Stream enumeration only surfaces resource/scope labels. Record attributes live in the per-record
	// column, so add their (normalized) key names from the engine's key index. They are window-global
	// rather than stream-scoped, so only fold them in for an unfiltered listing — a stream selector
	// can't soundly restrict them.
	if len(opts.Query.Matchers) == 0 {
		lo, hi := seriesWindow(opts.Start, opts.End)
		keys, err := q.b.store.LogKeys(ctx, q.b.tenant, lo, hi)
		if err != nil {
			return nil, errors.Wrap(err, "log keys")
		}
		for _, ki := range keys {
			if ki.Scope&storage.KeyScopeRecord != 0 {
				names[otelstorage.KeyToLabel(string(ki.Key))] = struct{}{}
			}
		}
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
