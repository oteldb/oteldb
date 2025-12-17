package promql

import (
	"context"
	"slices"
	"sync"

	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/oteldb/promql-engine/warnings"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type seriesSelector struct {
	storage  storage.Querier
	matchers []*labels.Matcher
	hints    storage.SelectHints

	once   sync.Once
	series []promscanners.SignedSeries
}

var _ promscanners.SeriesSelector = (*seriesSelector)(nil)

// NewSelector creates new [promscanners.SeriesSelector].
func NewSelector(q storage.Querier, matchers []*labels.Matcher, hints storage.SelectHints) promscanners.SeriesSelector {
	return &seriesSelector{
		storage:  q,
		matchers: matchers,
		hints:    hints,
	}
}

func (o *seriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *seriesSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return SeriesShard(o.series, shard, numShards), nil
}

func (o *seriesSelector) loadSeries(ctx context.Context) error {
	seriesSet := o.storage.Select(ctx, false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.series = append(o.series, promscanners.SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	for _, w := range seriesSet.Warnings() {
		warnings.AddToContext(w, ctx)
	}
	return seriesSet.Err()
}

type filteredSelector struct {
	selector promscanners.SeriesSelector
	filter   promscanners.Filter

	once   sync.Once
	series []promscanners.SignedSeries
}

var _ promscanners.SeriesSelector = (*filteredSelector)(nil)

// NewFilteredSelector creates new [promscanners.SeriesSelector] that applies filtering.
func NewFilteredSelector(q storage.Querier, matchers, filters []*labels.Matcher, hints storage.SelectHints) promscanners.SeriesSelector {
	return &filteredSelector{
		selector: NewSelector(q, matchers, hints),
		filter:   promscanners.NewFilter(filters),
	}
}

func (f *filteredSelector) Matchers() []*labels.Matcher {
	return slices.Concat(f.selector.Matchers(), f.filter.Matchers())
}

func (f *filteredSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	var err error
	f.once.Do(func() { err = f.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return SeriesShard(f.series, shard, numShards), nil
}

func (f *filteredSelector) loadSeries(ctx context.Context) error {
	series, err := f.selector.GetSeries(ctx, 0, 1)
	if err != nil {
		return err
	}

	var i uint64
	f.series = make([]promscanners.SignedSeries, 0, len(series))
	for _, s := range series {
		if f.filter.Matches(s) {
			f.series = append(f.series, promscanners.SignedSeries{
				Series:    s.Series,
				Signature: i,
			})
			i++
		}
	}

	return nil
}

// SeriesShard shards series using given index.
func SeriesShard(series []promscanners.SignedSeries, index, numShards int) []promscanners.SignedSeries {
	start := index * len(series) / numShards
	end := min((index+1)*len(series)/numShards, len(series))

	slice := series[start:end]
	shard := make([]promscanners.SignedSeries, len(slice))
	copy(shard, slice)

	for i := range shard {
		shard[i].Signature = uint64(i)
	}
	return shard
}
