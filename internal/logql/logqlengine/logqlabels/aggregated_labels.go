package logqlabels

import (
	"cmp"
	"iter"
	"maps"
	"regexp"
	"slices"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

type aggregatedLabels struct {
	entries []labelEntry
	without map[string]struct{}
	by      map[string]struct{}
}

type labelEntry struct {
	name  string
	value string
}

func sortEntries(entries []labelEntry) {
	slices.SortFunc(entries, func(a, b labelEntry) int {
		return cmp.Compare(a.name, b.name)
	})
}

// AggregatedLabelsFromSet creates new [AggregatedLabels] from [LabelSet].
func AggregatedLabelsFromSet(set LabelSet, by, without map[string]struct{}) AggregatedLabels {
	labels := make([]labelEntry, 0, set.Len())
	set.Range(func(l logql.Label, v pcommon.Value) {
		labels = append(labels, labelEntry{
			name:  string(l),
			value: v.AsString(),
		})
	})
	sortEntries(labels)

	return &aggregatedLabels{
		entries: labels,
		without: without,
		by:      by,
	}
}

// AggregatedLabelsFromMap creates new [AggregatedLabels] from label map.
func AggregatedLabelsFromMap(m map[string]string) AggregatedLabels {
	labels := make([]labelEntry, 0, len(m))
	for key, value := range m {
		labels = append(labels, labelEntry{name: key, value: value})
	}
	slices.SortFunc(labels, func(a, b labelEntry) int {
		return cmp.Compare(a.name, b.name)
	})

	return &aggregatedLabels{
		entries: labels,
		without: nil,
		by:      nil,
	}
}

// AggregatedLabelsFromSeq creates new [AggregatedLabels] from label sequence.
func AggregatedLabelsFromSeq(s iter.Seq2[string, string]) AggregatedLabels {
	var labels []labelEntry
	for key, value := range s {
		labels = append(labels, labelEntry{name: key, value: value})
	}
	slices.SortFunc(labels, func(a, b labelEntry) int {
		return cmp.Compare(a.name, b.name)
	})

	return &aggregatedLabels{
		entries: labels,
		without: nil,
		by:      nil,
	}
}

// By returns new set of labels containing only given list of labels.
func (a *aggregatedLabels) By(labels ...logql.Label) AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: a.without,
		by:      buildSet(maps.Clone(a.by), labels...),
	}
	return sub
}

// Without returns new set of labels without given list of labels.
func (a *aggregatedLabels) Without(labels ...logql.Label) AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: buildSet(maps.Clone(a.without), labels...),
		by:      a.by,
	}
	return sub
}

// Key computes grouping key from set of labels.
func (a *aggregatedLabels) Key() GroupingKey {
	h := xxhash.New()
	a.forEach(func(k, v string) {
		_, _ = h.WriteString(k)
		_, _ = h.WriteString(v)
	})
	return h.Sum64()
}

// Replace replaces labels using given regexp.
func (a *aggregatedLabels) Replace(dstLabel, replacement, srcLabel string, re *regexp.Regexp) AggregatedLabels {
	src := a.findEntry(srcLabel)

	idxs := re.FindStringSubmatchIndex(src)
	if idxs == nil {
		return a
	}

	dst := re.ExpandString(nil, replacement, src, idxs)
	if len(dst) == 0 {
		// Destination value is empty, delete it.
		a.deleteEntry(dstLabel)
	} else {
		a.setEntry(dstLabel, string(dst))
	}

	return a
}

func (a *aggregatedLabels) findEntry(key string) string {
	for _, e := range a.entries {
		if e.name == key {
			return e.value
		}
	}
	return ""
}

func (a *aggregatedLabels) deleteEntry(key string) {
	n := 0
	for _, e := range a.entries {
		if e.name == key {
			continue
		}
		a.entries[n] = e
		n++
	}
	a.entries = a.entries[:n]
}

func (a *aggregatedLabels) setEntry(key, value string) {
	var entry *labelEntry
	for i, e := range a.entries {
		if e.name == key {
			entry = &a.entries[i]
			break
		}
	}

	replacement := labelEntry{
		name:  key,
		value: value,
	}
	if entry == nil {
		a.entries = append(a.entries, replacement)
	} else {
		*entry = replacement
	}
	// TODO(tdakkota): suboptimal, probably should use heap/tree instead.
	sortEntries(a.entries)
}

// AsLokiAPI returns API structure for label set.
func (a *aggregatedLabels) AsLokiAPI() (r lokiapi.LabelSet) {
	r = lokiapi.LabelSet{}
	a.forEach(func(k, v string) {
		r[k] = v
	})
	return r
}

func (a *aggregatedLabels) forEach(cb func(k, v string)) {
	for _, e := range a.entries {
		if _, ok := a.without[e.name]; ok {
			continue
		}
		if len(a.by) > 0 {
			if _, ok := a.by[e.name]; !ok {
				continue
			}
		}
		cb(e.name, e.value)
	}
}

func buildSet[K ~string](r map[string]struct{}, input ...K) map[string]struct{} {
	if len(input) == 0 {
		return r
	}

	if r == nil {
		r = make(map[string]struct{}, len(input))
	}
	for _, k := range input {
		r[string(k)] = struct{}{}
	}
	return r
}
