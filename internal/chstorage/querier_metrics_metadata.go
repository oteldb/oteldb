package chstorage

import (
	"context"

	"github.com/oteldb/oteldb/internal/metricstorage"
)

var _ metricstorage.MetadataQuerier = (*Querier)(nil)

// MetricMetadata returns metric metadata for the given options.
func (q *Querier) MetricMetadata(ctx context.Context, opts metricstorage.MetadataParams) (metricstorage.Metadata, error) {
	return q.timeseries.QueryMetadata(ctx, opts)
}
