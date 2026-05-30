package logqlengine

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// KeepLabels label filtering Processor.
type KeepLabels struct {
	keep     map[logql.Label]struct{}
	matchers map[logql.Label][]StringMatcher
}

func buildKeepLabels(stage *logql.KeepLabelsExpr) (Processor, error) {
	e := &KeepLabels{}
	if labels := stage.Labels; len(labels) > 0 {
		e.keep = make(map[logql.Label]struct{}, len(labels))
		for _, label := range labels {
			e.keep[label] = struct{}{}
		}
	}
	if matchers := stage.Matchers; len(matchers) > 0 {
		e.matchers = make(map[logql.Label][]StringMatcher, len(matchers))
		for _, matcher := range matchers {
			label := matcher.Label
			m, err := buildStringMatcher(matcher.Op, matcher.Value, matcher.Re, true)
			if err != nil {
				return nil, err
			}
			e.matchers[label] = append(e.matchers[label], m)
		}
	}
	return e, nil
}

// Process implements Processor.
func (k *KeepLabels) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	set.Range(func(label logql.Label, val pcommon.Value) {
		if !k.keepPair(label, val) {
			set.Delete(label)
		}
	})
	return line, true
}

func (k *KeepLabels) keepPair(label logql.Label, val pcommon.Value) bool {
	_, ok1 := k.keep[label]
	ms, ok2 := k.matchers[label]

	if !ok1 && !ok2 {
		return false
	}

	for _, m := range ms {
		if !m.Match(val.AsString()) {
			return false
		}
	}
	return true
}
