// Package metricstorage defines some interfaces for metric storage.
package metricstorage

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// MetricName is a special label name that represent a metric name.
const MetricName = "__name__"

// OptimizedSeriesQuerier defines API for optimal series querying.
type OptimizedSeriesQuerier interface {
	OnlySeries(ctx context.Context, sortSeries bool, startMs, endMs int64, matcherSets ...[]*labels.Matcher) storage.SeriesSet
}
