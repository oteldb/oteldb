package chstorage

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

var _ storage.Queryable = (*Querier)(nil)

// Querier returns a new Querier on the storage.
func (q *Querier) Querier(mint, maxt int64) (storage.Querier, error) {
	var minTime, maxTime time.Time

	// In case if Prometheus passes min/max time, keep it zero.
	if mint != promapi.MinTime.UnixMilli() {
		minTime = time.UnixMilli(mint)
	}
	if maxt != promapi.MaxTime.UnixMilli() {
		maxTime = time.UnixMilli(maxt)
	}
	return &promQuerier{
		mint: minTime,
		maxt: maxTime,

		tables:          q.tables,
		labelLimit:      q.labelLimit,
		queryTimeseries: q.timeseries.Query,
		do:              q.do,

		tracer: q.tracer,
	}, nil
}

type promQuerier struct {
	mint time.Time
	maxt time.Time

	tables     Tables
	labelLimit int

	queryTimeseries queryMetricsTimeseriesFunc
	do              func(ctx context.Context, s selectQuery) error

	tracer trace.Tracer
}

var _ storage.Querier = (*promQuerier)(nil)

// Close releases the resources of the Querier.
func (p *promQuerier) Close() error {
	return nil
}

func (p *promQuerier) getStart(t time.Time) time.Time {
	switch {
	case t.IsZero():
		return p.mint
	case p.mint.IsZero():
		return t
	case t.After(p.mint):
		return t
	default:
		return p.mint
	}
}

func (p *promQuerier) getEnd(t time.Time) time.Time {
	switch {
	case t.IsZero():
		return p.maxt
	case p.maxt.IsZero():
		return t
	case t.Before(p.maxt):
		return t
	default:
		return p.maxt
	}
}

// DecodeUnicodeLabel tries to decode U__k8s_2e_node_2e_name into k8s.node.name.
// It decodes any hex-encoded character in the format _XX_ where XX is a two-digit hex value.
func DecodeUnicodeLabel(v string) string {
	if !strings.HasPrefix(v, "U__") {
		return v
	}
	var (
		sb    strings.Builder
		runes = []rune(v[3:]) // Skip U__
	)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '_' && i+3 < len(runes) && runes[i+3] == '_' {
			// Try to decode _XX_ where XX is hex
			hex := string([]rune{runes[i+1], runes[i+2]})
			if b, err := strconv.ParseUint(hex, 16, 8); err == nil {
				sb.WriteByte(byte(b))
				i += 3 // Skip _XX_
			} else {
				sb.WriteRune(runes[i])
			}
		} else {
			sb.WriteRune(runes[i])
		}
	}
	return sb.String()
}

func promQLLabelMatcher(valueSel []chsql.Expr, typ labels.MatchType, value string) (e chsql.Expr, rerr error) {
	defer func() {
		if rerr == nil {
			switch typ {
			case labels.MatchNotEqual, labels.MatchNotRegexp:
				e = chsql.Not(e)
			}
		}
	}()

	// Note: predicate negated above.
	var (
		valueExpr = chsql.String(value)
		exprs     = make([]chsql.Expr, 0, len(valueSel))
	)
	switch typ {
	case labels.MatchEqual, labels.MatchNotEqual:
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.Eq(sel, valueExpr))
		}
	case labels.MatchRegexp, labels.MatchNotRegexp:
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.Match(sel, valueExpr))
		}
	default:
		return e, errors.Errorf("unexpected type %q", typ)
	}

	return chsql.JoinOr(exprs...), nil
}

func timeseriesInRange(query *chsql.SelectQuery, start, end time.Time, prec proto.Precision) {
	if !start.IsZero() {
		query.Having(chsql.Gte(
			chsql.Function("max", chsql.Ident("last_seen")),
			chsql.DateTime64(start, prec),
		))
	}
	if !end.IsZero() {
		query.Having(chsql.Lte(
			chsql.Function("min", chsql.Ident("first_seen")),
			chsql.DateTime64(end, prec),
		))
	}
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select, but it's up to implementation how this is used if used at all.
func (p *promQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if hints != nil && hints.Func == "series" {
		ss, err := p.selectOnlySeries(ctx, sortSeries, hints.Start, hints.End, matchers)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return ss
	}

	ss, err := p.selectSeries(ctx, sortSeries, hints, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return ss
}

func (p *promQuerier) selectSeries(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (_ storage.SeriesSet, rerr error) {
	hints, start, end, queryLabels := p.extractHints(hints, matchers)

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.selectSeries",
		trace.WithAttributes(
			attribute.Bool("promql.sort_series", sortSeries),
			attribute.Int64("promql.hints.start", hints.Start),
			attribute.Int64("promql.hints.end", hints.End),
			attribute.Int64("promql.hints.step", hints.Step),
			attribute.String("promql.hints.func", hints.Func),
			attribute.StringSlice("promql.hints.grouping", hints.Grouping),
			attribute.Bool("promql.hints.by", hints.By),
			attribute.Int64("promql.hints.range", hints.Range),
			attribute.String("promql.hints.shard_count", strconv.FormatUint(hints.ShardCount, 10)),
			attribute.String("promql.hints.shard_index", strconv.FormatUint(hints.ShardIndex, 10)),
			attribute.Bool("promql.hints.disable_trimming", hints.DisableTrimming),
			xattribute.StringerSlice("promql.matchers", matchers),

			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.StringSlice("chstorage.matchers.labels", queryLabels),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	timeseries, err := p.queryTimeseries(ctx, [][]*labels.Matcher{matchers})
	if err != nil {
		return nil, errors.Wrap(err, "query timeseries hashes")
	}

	var (
		points        []storage.Series
		expHistSeries []storage.Series
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		result, err := p.queryPoints(ctx, p.tables.Points, start, end, timeseries)
		if err != nil {
			return errors.Wrap(err, "query points")
		}

		points = result
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		result, err := p.queryExpHistograms(ctx, p.tables.ExpHistograms, start, end, timeseries)
		if err != nil {
			return errors.Wrap(err, "query exponential histograms")
		}

		expHistSeries = result
		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	points = append(points, expHistSeries...)
	if sortSeries {
		slices.SortFunc(points, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}
	return newSeriesSet(points), nil
}

func (p *promQuerier) extractHints(
	hints *storage.SelectHints,
	matchers []*labels.Matcher,
) (_ *storage.SelectHints, start, end time.Time, mlabels []string) {
	if hints != nil {
		if ms := hints.Start; ms != promapi.MinTime.UnixMilli() {
			start = p.getStart(time.UnixMilli(ms))
		}
		if ms := hints.End; ms != promapi.MaxTime.UnixMilli() {
			end = p.getEnd(time.UnixMilli(ms))
		}
	} else {
		hints = new(storage.SelectHints)
	}

	mlabels = make([]string, 0, len(matchers))
	for _, m := range matchers {
		mlabels = append(mlabels, m.Name)
	}

	return hints, start, end, mlabels
}

func (p *promQuerier) queryPoints(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ []storage.Series, rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryPoints",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		c     = newPointColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.InTimeRange("timestamp", start, end, c.timestamp.Precision),
				chsql.In(
					chsql.Ident("hash"),
					chsql.Ident("timeseries_hashes"),
				),
			).
			Order(chsql.ToStartOfHour(chsql.Ident("timestamp")), chsql.Asc).
			Order(chsql.Ident("hash"), chsql.Asc).
			Order(chsql.Ident("timestamp"), chsql.Asc)

		inputData proto.ColFixedStr16
	)
	for hash := range timeseries {
		inputData.Append(hash)
	}

	var (
		set         = map[[16]byte]*series[pointData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         query,
		ExternalTable: "timeseries_hashes",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					hash      = c.hash.Row(i)
					value     = c.value.Row(i)
					timestamp = c.timestamp.Row(i)
				)
				s, ok := set[hash]
				if !ok {
					lb, ok := timeseries[hash]
					if !ok {
						zctx.From(ctx).Error("Can't find labels for requested series")
						continue
					}
					s = &series[pointData]{
						labels: lb,
					}
					set[hash] = s
				}

				s.data.values = append(s.data.values, value)
				s.ts = append(s.ts, timestamp.UnixMilli())

				totalPoints++
			}
			return nil
		},

		Type:   "QueryPoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("points_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_points", totalPoints),
	))

	result := make([]storage.Series, 0, len(set))
	for _, s := range set {
		result = append(result, s)
	}
	return result, nil
}

func (p *promQuerier) queryExpHistograms(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ []storage.Series, rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryExpHistograms",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		c     = newExpHistogramColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.InTimeRange("timestamp", start, end, c.timestamp.Precision),
				chsql.In(
					chsql.Ident("hash"),
					chsql.Ident("timeseries_hashes"),
				),
			).
			Order(chsql.ToStartOfHour(chsql.Ident("timestamp")), chsql.Asc).
			Order(chsql.Ident("hash"), chsql.Asc).
			Order(chsql.Ident("timestamp"), chsql.Asc)

		inputData proto.ColFixedStr16
	)
	for hash := range timeseries {
		inputData.Append(hash)
	}

	var (
		set         = map[[16]byte]*series[expHistData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         query,
		ExternalTable: "timeseries_hashes",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					hash                 = c.hash.Row(i)
					timestamp            = c.timestamp.Row(i)
					count                = c.count.Row(i)
					sum                  = c.sum.Row(i)
					vmin                 = c.min.Row(i)
					vmax                 = c.max.Row(i)
					scale                = c.scale.Row(i)
					zerocount            = c.zerocount.Row(i)
					positiveOffset       = c.positiveOffset.Row(i)
					positiveBucketCounts = c.positiveBucketCounts.Row(i)
					negativeOffset       = c.negativeOffset.Row(i)
					negativeBucketCounts = c.negativeBucketCounts.Row(i)
				)
				s, ok := set[hash]
				if !ok {
					lb, ok := timeseries[hash]
					if !ok {
						zctx.From(ctx).Error("Can't find labels for requested series")
						continue
					}
					s = &series[expHistData]{
						labels: lb,
					}
					set[hash] = s
				}

				s.data.count = append(s.data.count, count)
				s.data.sum = append(s.data.sum, sum)
				s.data.min = append(s.data.min, vmin)
				s.data.max = append(s.data.max, vmax)
				s.data.scale = append(s.data.scale, scale)
				s.data.zerocount = append(s.data.zerocount, zerocount)
				s.data.positiveOffset = append(s.data.positiveOffset, positiveOffset)
				s.data.positiveBucketCounts = append(s.data.positiveBucketCounts, positiveBucketCounts)
				s.data.negativeOffset = append(s.data.negativeOffset, negativeOffset)
				s.data.negativeBucketCounts = append(s.data.negativeBucketCounts, negativeBucketCounts)
				s.ts = append(s.ts, timestamp.UnixMilli())

				totalPoints++
			}
			return nil
		},

		Type:   "QueryExpHistograms",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("histograms_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_points", totalPoints),
	))

	result := make([]storage.Series, 0, len(set))
	for _, s := range set {
		result = append(result, s)
	}
	return result, nil
}

func buildPromLabels(lb *labels.ScratchBuilder, set map[string]string) labels.Labels {
	lb.Reset()
	for key, value := range set {
		lb.Add(key, value)
	}
	lb.Sort()
	return lb.Labels()
}

type seriesSet[S storage.Series] struct {
	set []S
	n   int
}

func newSeriesSet[S storage.Series](set []S) *seriesSet[S] {
	return &seriesSet[S]{
		set: set,
		n:   -1,
	}
}

var _ storage.SeriesSet = (*seriesSet[storage.Series])(nil)

func (s *seriesSet[S]) Next() bool {
	if s.n+1 >= len(s.set) {
		return false
	}
	s.n++
	return true
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s *seriesSet[S]) At() storage.Series {
	return s.set[s.n]
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (s *seriesSet[S]) Err() error {
	return nil
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (s *seriesSet[S]) Warnings() annotations.Annotations {
	return nil
}

type seriesData interface {
	Iterator(ts []int64) chunkenc.Iterator
}

type series[Data seriesData] struct {
	labels labels.Labels
	data   Data
	ts     []int64
}

var _ storage.Series = (*series[pointData])(nil)

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s *series[Data]) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s *series[Data]) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return s.data.Iterator(s.ts)
}
