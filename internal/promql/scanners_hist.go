package promql

import (
	"context"

	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type histogramStatsSelector struct {
	promscanners.SeriesSelector
}

var _ promscanners.SeriesSelector = histogramStatsSelector{}

// NewHistogramStatsSelector creates new [promscanners.SeriesSelector] that decodes native histograms.
func NewHistogramStatsSelector(seriesSelector promscanners.SeriesSelector) promscanners.SeriesSelector {
	return histogramStatsSelector{SeriesSelector: seriesSelector}
}

func (h histogramStatsSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	series, err := h.SeriesSelector.GetSeries(ctx, shard, numShards)
	if err != nil {
		return nil, err
	}
	for i := range series {
		series[i].Series = newHistogramStatsSeries(series[i].Series)
	}
	return series, nil
}

type histogramStatsSeries struct {
	storage.Series
}

var _ storage.Series = histogramStatsSeries{}

func newHistogramStatsSeries(series storage.Series) histogramStatsSeries {
	return histogramStatsSeries{Series: series}
}

func (h histogramStatsSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return promql.NewHistogramStatsIterator(h.Series.Iterator(it))
}
