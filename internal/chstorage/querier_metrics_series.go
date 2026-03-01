package chstorage

import (
	"context"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// OnlySeries selects only labels from series.
func (p *promQuerier) OnlySeries(ctx context.Context, sortSeries bool, startMs, endMs int64, matcherSets ...[]*labels.Matcher) storage.SeriesSet {
	ss, err := p.selectOnlySeries(ctx, sortSeries, startMs, endMs, matcherSets...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return ss
}

var _ metricstorage.OptimizedSeriesQuerier = (*promQuerier)(nil)

// OnlySeries selects only labels from series.
func (p *promQuerier) selectOnlySeries(
	ctx context.Context,
	sortSeries bool,
	startMs, endMs int64,
	matcherSets ...[]*labels.Matcher,
) (_ storage.SeriesSet, rerr error) {
	var start, end time.Time
	if ms := startMs; ms != promapi.MinTime.UnixMilli() {
		start = p.getStart(time.UnixMilli(ms))
	}
	if ms := endMs; ms != promapi.MaxTime.UnixMilli() {
		end = p.getEnd(time.UnixMilli(ms))
	}

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.selectOnlySeries",
		trace.WithAttributes(
			attribute.Bool("promql.sort_series", sortSeries),
			attribute.Int64("promql.hints.start", startMs),
			attribute.Int64("promql.hints.end", endMs),

			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		table  = p.tables.Timeseries
		series = proto.ColMap[string, string]{
			Keys:   new(proto.ColStr),
			Values: new(proto.ColStr),
		}
	)
	query, err := p.buildSeriesQuery(
		table,
		&series,
		start, end,
		matcherSets,
	)
	if err != nil {
		return nil, errors.Wrap(err, "build series query")
	}

	var (
		result []onlyLabelsSeries

		dedup = map[string]string{}
		lb    labels.ScratchBuilder
	)
	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < series.Rows(); i++ {
				clear(dedup)
				forEachColMap(&series, i, func(k, v string) {
					dedup[k] = v
				})

				lb.Reset()
				for k, v := range dedup {
					lb.Add(k, v)
				}
				lb.Sort()
				result = append(result, onlyLabelsSeries{
					labels: lb.Labels(),
				})
			}
			return nil
		},

		Type:   "QueryOnlySeries",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("series_fetched", trace.WithAttributes(
		attribute.String("chstorage.table", table),
		attribute.Int("chstorage.total_series", len(result)),
	))

	if sortSeries {
		slices.SortFunc(result, func(a, b onlyLabelsSeries) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}
	return newSeriesSet(result), nil
}

func (p *promQuerier) buildSeriesQuery(
	table string,
	column proto.ColResult,
	start, end time.Time,
	matcherSets [][]*labels.Matcher,
) (*chsql.SelectQuery, error) {
	query := chsql.Select(table,
		chsql.ResultColumn{
			Name: "series",
			Expr: chsql.MapConcat(
				chsql.Map(chsql.String("__name__"), chsql.Ident("name")),
				attrStringMap(colAttrs),
				attrStringMap(colResource),
				attrStringMap(colScope),
			),
			Data: column,
		}).
		Distinct(true)

	sets := make([]chsql.Expr, 0, len(matcherSets))
	for _, set := range matcherSets {
		matchers := make([]chsql.Expr, 0, len(set))
		for _, m := range set {
			selector := chsql.Ident("name")
			if name := m.Name; name != metricstorage.MetricName {
				selector = firstAttrSelector(name)
			}
			expr, err := promQLLabelMatcher([]chsql.Expr{selector}, m.Type, m.Value)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, expr)
		}
		sets = append(sets, chsql.JoinAnd(matchers...))
	}
	query.Where(chsql.JoinOr(sets...))

	query.GroupBy(
		chsql.Ident("name"),
		chsql.Ident("attribute"),
		chsql.Ident("scope"),
		chsql.Ident("resource"),
	)
	timeseriesInRange(query, start, end, newTimeseriesColumns().timestampPrecision())

	return query, nil
}

type onlyLabelsSeries struct {
	labels labels.Labels
}

var _ storage.Series = onlyLabelsSeries{}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s onlyLabelsSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (onlyLabelsSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}
