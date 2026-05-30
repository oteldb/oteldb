package logqlengine

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// DropLabels label filtering Processor.
type DropLabels struct {
	drop     map[logql.Label]struct{}
	matchers map[logql.Label][]StringMatcher
}

func buildDropLabels(stage *logql.DropLabelsExpr) (Processor, error) {
	e := &DropLabels{}
	if labels := stage.Labels; len(labels) > 0 {
		e.drop = make(map[logql.Label]struct{}, len(labels))
		for _, label := range labels {
			e.drop[label] = struct{}{}
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
func (k *DropLabels) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	set.Range(func(label logql.Label, val pcommon.Value) {
		if k.dropPair(label, val) {
			set.Delete(label)
		}
	})
	return line, true
}

func (k *DropLabels) dropPair(label logql.Label, val pcommon.Value) bool {
	_, ok1 := k.drop[label]
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
