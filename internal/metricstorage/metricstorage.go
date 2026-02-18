// Package metricstorage defines some interfaces for metric storage.
package metricstorage

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/go-faster/oteldb/internal/promapi"
)

// MetricName is a special label name that represent a metric name.
const MetricName = "__name__"

// OptimizedSeriesQuerier defines API for optimal series querying.
type OptimizedSeriesQuerier interface {
	OnlySeries(ctx context.Context, sortSeries bool, startMs, endMs int64, matcherSets ...[]*labels.Matcher) storage.SeriesSet
}

// MetadataQuerier defines API for querying metric metadata.
type MetadataQuerier interface {
	MetricMetadata(ctx context.Context, opts MetadataParams) (Metadata, error)
}

// MetadataParams defines options for querying metric metadata.
type MetadataParams struct {
	// MetricName is the name of the metric to query metadata for.
	//
	// If empty, metadata for all metrics will be returned.
	MetricName string
	// Limit is the maximum number of metadata entries to return.
	//
	// If zero or less, all metadata entries will be returned.
	Limit int
}

// Metadata is a map of metric names to their metadata.
type Metadata = map[string]MetricMetadata

// MetricMetadata describes the metadata of a metric, including its type, help string, and unit.
type MetricMetadata struct {
	Type promapi.MetricMetadataType
	Help string
	Unit string
}
