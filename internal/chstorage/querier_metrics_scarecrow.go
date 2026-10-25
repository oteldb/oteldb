package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"

	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// ScarecrowScanner preserves ClickHouse's rate-family SQL offloads for Scarecrow.
func (q *Querier) ScarecrowScanner() scarecrow.Scanner {
	return &scarecrowScanner{Scanner: scarecrow.NewQueryableScanner(scarecrowQueryable{q}), querier: q}
}

type scarecrowScanner struct {
	scarecrow.Scanner
	querier *Querier
}

var _ scarecrow.RangeFunctionScanner = (*scarecrowScanner)(nil)

func (s *scarecrowScanner) SupportsRangeFunction(name string) bool {
	_, ok := funcNameToRateKind(name)
	return ok && !s.querier.disableRateOffloading && !s.querier.disableMetricOffloading
}

func (s *scarecrowScanner) RangeFunction(
	ctx context.Context, name string, grid scarecrow.WindowGrid, matchers []*labels.Matcher,
) ([]scarecrow.Samples, error) {
	end := grid.Start + int64(grid.NumSteps-1)*grid.Step
	q := s.querier.metricsQuerier(grid.Start-grid.Width, end)
	queryTimeseries := q.queryTimeseries
	q.queryTimeseries = func(ctx context.Context, _, _ time.Time, sets [][]*labels.Matcher) (metricsTimeseries, error) {
		start := time.UnixMilli(grid.Start - grid.Width + 1)
		timeseries, err := queryTimeseries(ctx, start, time.UnixMilli(end), sets)
		if err != nil || len(timeseries) == 0 {
			return timeseries, err
		}
		hasHistograms, err := q.hasScarecrowHistograms(ctx, start, time.UnixMilli(end), timeseries)
		if err != nil {
			return nil, err
		}
		if hasHistograms {
			return nil, errors.Wrap(scarecrow.ErrUnsupported, "native histogram samples")
		}
		return timeseries, nil
	}
	result, err := q.querySeriesSingleflight(ctx, true, metricSelectParams{
		Matchers: matchers,
		Start:    time.UnixMilli(grid.Start), End: time.UnixMilli(end),
		Step:     time.Duration(grid.Step) * time.Millisecond,
		Range:    time.Duration(grid.Width) * time.Millisecond,
		Function: name,
	})
	if err != nil {
		return nil, err
	}
	out := make([]scarecrow.Samples, 0, len(result.points))
	for _, series := range result.points {
		builder := labels.NewBuilder(series.labels)
		builder.Del(prommodel.MetricNameLabel)
		out = append(out, scarecrow.Samples{
			Labels: builder.Labels(), T: series.ts, V: series.data.values,
		})
	}
	return out, nil
}

type scarecrowQueryable struct{ q *Querier }

func (q scarecrowQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return scarecrowQuerier{q.q.metricsQuerier(mint, maxt)}, nil
}

type scarecrowQuerier struct{ *promQuerier }

func (q scarecrowQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	set := q.promQuerier.Select(ctx, sorted, hints, matchers...)
	if seriesSet, ok := set.(*seriesSet[storage.Series]); ok {
		for _, item := range seriesSet.set {
			if _, ok := item.(*series[expHistData]); ok {
				return storage.ErrSeriesSet(errors.Wrap(scarecrow.ErrUnsupported, "native histogram samples"))
			}
		}
	}
	return set
}

func (p *promQuerier) hasScarecrowHistograms(ctx context.Context, start, end time.Time, series metricsTimeseries) (bool, error) {
	var hashes, matches proto.ColFixedStr16
	for hash := range series {
		hashes.Append(hash)
	}
	table := p.tables.ExpHistograms
	query := chsql.Select(table, chsql.Column("hash", &matches)).Where(
		chsql.InTimeRange("timestamp", start, end, proto.PrecisionNano),
		chsql.In(chsql.Ident("hash"), chsql.Ident("timeseries_hashes")),
	).Limit(1)
	found := false
	err := p.do(ctx, selectQuery{
		Query: query, ExternalTable: "timeseries_hashes",
		ExternalData: []proto.InputColumn{{Name: "hash", Data: &hashes}},
		OnResult:     func(context.Context, proto.Block) error { found = found || matches.Rows() > 0; return nil },
		Type:         "CheckNativeHistograms", Signal: "metrics", Table: table,
	})
	return found, err
}
