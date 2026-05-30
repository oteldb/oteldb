package logqlengine

import (
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

type distinctKey struct {
	label logql.Label
	value string
}

// DistinctFilter filters out records with duplicate label values.
type DistinctFilter struct {
	labels []logql.Label
	state  map[distinctKey]struct{}
}

func buildDistinctFilter(stage *logql.DistinctFilter) (Processor, error) {
	return &DistinctFilter{
		labels: stage.Labels,
		state:  map[distinctKey]struct{}{},
	}, nil
}

// Process implements Processor.
func (d *DistinctFilter) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	for _, label := range d.labels {
		val, ok := set.GetString(label)
		if !ok {
			return line, true
		}

		key := distinctKey{
			label: label,
			value: val,
		}
		if _, ok := d.state[key]; ok {
			return line, false
		}
		d.state[key] = struct{}{}

		keep = true
	}
	return line, keep
}
