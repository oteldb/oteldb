package scarecrow

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func hasUngroupedSeries(schema *Schema) bool {
	for _, ls := range schema.Series {
		if dropMetricName(ls).IsEmpty() {
			return true
		}
	}
	return false
}

func combinedMetricName(out, left, right labels.Labels, op parser.ItemType) labels.Labels {
	leftName, rightName := left.Get(model.MetricNameLabel), right.Get(model.MetricNameLabel)
	if leftName == "" || rightName == "" {
		return out
	}
	b := labels.NewBuilder(out)
	b.Set(model.MetricNameLabel, fmt.Sprintf("(%s %s %s)", leftName, op, rightName))
	return b.Labels()
}
