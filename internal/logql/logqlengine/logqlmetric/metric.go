package logqlmetric

import (
	"math"

	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// FPoint is a metric point.
type FPoint struct {
	Timestamp otelstorage.Timestamp
	Value     float64
}

// Sample is a metric sample extracted from logs.
type Sample struct {
	Data float64
	Set  logqlabels.AggregatedLabels
}

// Less compares two samples by value.
func (a Sample) Less(b Sample) bool {
	return math.IsNaN(a.Data) || a.Data < b.Data
}

// Greater compares two samples by value.
func (a Sample) Greater(b Sample) bool {
	return math.IsNaN(a.Data) || a.Data > b.Data
}

// Series is a grouped set of metric points.
type Series struct {
	Data []FPoint
	Set  logqlabels.AggregatedLabels
}

// SampledEntry is a sampled log entry.
type SampledEntry struct {
	Sample    float64
	Timestamp otelstorage.Timestamp
	Set       logqlabels.AggregatedLabels
}
