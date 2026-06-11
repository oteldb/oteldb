package chstorage

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	singleflight "github.com/go-faster/sdk/singleflightx"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/metricscache"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/xattribute"
)

var _ storage.Queryable = (*Querier)(nil)

// Querier returns a new metrics [storage.Querier].
func (q *Querier) Querier(mint, maxt int64) (storage.Querier, error) {
	return q.metricsQuerier(mint, maxt), nil
}

type (
	queryPointsFunc                 = func(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (map[[16]byte]*series[pointData], error)
	querySampledPointsPerSeriesFunc = func(
		ctx context.Context,
		sampler pointsSampler,
		start, end time.Time,
		step time.Duration,
		timeseries map[[16]byte]labels.Labels,
	) (map[[16]byte]*series[pointData], error)
	queryRatePointsByHashFunc = func(
		ctx context.Context,
		start, end time.Time,
		step, window, offset time.Duration,
		timeseries map[[16]byte]labels.Labels,
		kind rateKind,
	) (map[[16]byte]*series[pointData], error)
)

type promQuerier struct {
	mint time.Time
	maxt time.Time

	tables          Tables
	labelLimit      int
	timeseriesLimit int

	metricsCache                    *metricscache.Cache
	queryTimeseries                 queryMetricsTimeseriesFunc
	queryPointsFunc                 queryPointsFunc
	querySampledPointsPerSeriesFunc querySampledPointsPerSeriesFunc
	queryRatePointsByHashFunc       queryRatePointsByHashFunc
	metricsSg                       *singleflight.Group[xxh3.Uint128, metricSelectResult]
	do                              func(ctx context.Context, s selectQuery) error

	disableRateOffloading   bool
	disableMetricOffloading bool

	tracer trace.Tracer
}

var _ storage.Querier = (*promQuerier)(nil)

func (q *Querier) metricsQuerier(mint, maxt int64) *promQuerier {
	var minTime, maxTime time.Time

	// In case if Prometheus passes min/max time, keep it zero.
	if mint != promapi.MinTime.UnixMilli() {
		minTime = time.UnixMilli(mint)
	}
	if maxt != promapi.MaxTime.UnixMilli() {
		maxTime = time.UnixMilli(maxt)
	}
	p := &promQuerier{
		mint: minTime,
		maxt: maxTime,

		tables:          q.tables,
		labelLimit:      q.labelLimit,
		timeseriesLimit: q.timeseriesLimit,

		disableRateOffloading:   q.disableRateOffloading,
		disableMetricOffloading: q.disableMetricOffloading,

		metricsCache:    q.metricsCache,
		metricsSg:       q.metricsSg,
		queryTimeseries: q.timeseries.Query,
		do:              q.do,

		tracer: q.tracer,
	}
	p.queryPointsFunc = p.queryPoints
	p.querySampledPointsPerSeriesFunc = p.querySampledPointsPerSeries
	p.queryRatePointsByHashFunc = p.queryRatePointsByHash
	return p
}

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
			// Try to decode _XX_ where XX is hexNumber
			hexNumber := string([]rune{runes[i+1], runes[i+2]})
			if b, err := strconv.ParseUint(hexNumber, 16, 8); err == nil {
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
	var (
		valueExpr = chsql.String(value)
		exprs     = make([]chsql.Expr, 0, len(valueSel))
	)
	switch typ {
	case labels.MatchEqual:
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.Eq(sel, valueExpr))
		}
	case labels.MatchNotEqual:
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.NotEq(sel, valueExpr))
		}
	case labels.MatchRegexp:
		// PromQL requires full-string match semantics, but ClickHouse's match()
		// uses RE2 search semantics (matches anywhere). Anchor the pattern.
		anchoredExpr := chsql.String("^(?:" + value + ")$")
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.Match(sel, anchoredExpr))
		}
	case labels.MatchNotRegexp:
		// Same anchoring required for negative regex matchers.
		anchoredExpr := chsql.String("^(?:" + value + ")$")
		for _, sel := range valueSel {
			exprs = append(exprs, chsql.Not(chsql.Match(sel, anchoredExpr)))
		}
	default:
		return e, errors.Errorf("unexpected type %q", typ)
	}

	return chsql.JoinOr(exprs...), nil
}

func timeseriesInRange(query *chsql.SelectQuery, start, end time.Time, prec proto.Precision) {
	if !start.IsZero() {
		// Add some lag to start time to handle inconsistency in case of imprecise timestamps.
		start = start.Add(-time.Minute)
		query.Having(chsql.Gte(
			chsql.Function("max", chsql.Ident("last_seen")),
			chsql.DateTime64(start, prec),
		))
	}
	if !end.IsZero() {
		// Also add some lag to end time.
		end = end.Add(time.Minute)
		query.Having(chsql.Lte(
			chsql.Function("min", chsql.Ident("first_seen")),
			chsql.DateTime64(end, prec),
		))
	}
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select, but it's up to implementation how this is used if used at all.
func (p *promQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (resultSet storage.SeriesSet) {
	hints, start, end := p.extractHints(hints)

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.Select",
		trace.WithAttributes(
			attribute.Bool("promql.sort_series", sortSeries),
			attribute.Int64("promql.hints.start", hints.Start),
			attribute.Int64("promql.hints.end", hints.End),
			attribute.Int("promql.hints.limit", hints.Limit),
			attribute.Int64("promql.hints.step", hints.Step),
			attribute.String("promql.hints.func", hints.Func),
			attribute.StringSlice("promql.hints.grouping", hints.Grouping),
			attribute.Bool("promql.hints.by", hints.By),
			attribute.Int64("promql.hints.range", hints.Range),
			attribute.String("promql.hints.shard_count", strconv.FormatUint(hints.ShardCount, 10)),
			attribute.String("promql.hints.shard_index", strconv.FormatUint(hints.ShardIndex, 10)),
			attribute.Bool("promql.hints.disable_trimming", hints.DisableTrimming),
			xattribute.StringerSlice("promql.matchers", matchers),
		),
	)
	defer func() {
		if resultSet != nil && resultSet.Err() != nil {
			span.RecordError(resultSet.Err())
		}
		span.End()
	}()

	if hints.Func == "series" {
		ss, err := p.selectOnlySeries(ctx, sortSeries, hints.Start, hints.End, matchers)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return ss
	}

	r, err := p.querySeriesSingleflight(ctx, true, metricSelectParams{
		Matchers:        matchers,
		Step:            time.Duration(hints.Step) * time.Millisecond,
		Start:           start,
		End:             end,
		Range:           time.Duration(hints.Range) * time.Millisecond,
		LookbackDelta:   0,
		Function:        hints.Func,
		SelectTimestamp: false,
		GroupBy:         hints.By,
		Grouping:        hints.Grouping,
	})
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return r.Set(sortSeries)
}

func (p *promQuerier) extractHints(hints *storage.SelectHints) (_ *storage.SelectHints, start, end time.Time) {
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
	return hints, start, end
}

type metricSelectParams struct {
	Matchers   []*labels.Matcher
	Step       time.Duration
	Start, End time.Time

	Range         time.Duration
	LookbackDelta time.Duration
	Offset        time.Duration

	Function string

	SelectTimestamp bool
	GroupBy         bool
	Grouping        []string
}

func (p *metricSelectParams) Hash(samplePoints bool) xxh3.Uint128 {
	const sep = ","
	writeBool := func(h *xxh3.Hasher, val bool) {
		if val {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.WriteString(sep)
	}
	writeInt64 := func(h *xxh3.Hasher, val int64) {
		_, _ = h.WriteString(strconv.FormatInt(val, 10))
		_, _ = h.WriteString(sep)
	}
	writeString := func(h *xxh3.Hasher, val string) {
		_, _ = h.WriteString(val)
		_, _ = h.WriteString(sep)
	}
	writeStrings := func(h *xxh3.Hasher, ss []string) {
		for i, v := range ss {
			if i != 0 {
				_, _ = h.WriteString(";")
			}
			_, _ = h.WriteString(v)
		}
		_, _ = h.WriteString(sep)
	}

	h := xxh3.New()
	hashPrometheusMatchers(h, [][]*labels.Matcher{p.Matchers})
	writeInt64(h, p.Step.Milliseconds())
	writeInt64(h, p.Start.UnixMilli())
	writeInt64(h, p.End.UnixMilli())
	writeInt64(h, p.Range.Milliseconds())
	writeInt64(h, p.LookbackDelta.Milliseconds())
	writeInt64(h, p.Offset.Milliseconds())
	writeString(h, p.Function)
	writeBool(h, p.SelectTimestamp)
	writeBool(h, p.GroupBy)
	writeStrings(h, p.Grouping)
	writeBool(h, samplePoints)
	return h.Sum128()
}

type metricSelectResult struct {
	points  []*series[pointData]
	expHist []*series[expHistData]
}

func (r *metricSelectResult) Set(sortSeries bool) *seriesSet[storage.Series] {
	result := make([]storage.Series, 0, len(r.points)+len(r.expHist))
	for _, s := range r.points {
		result = append(result, s)
	}
	for _, s := range r.expHist {
		result = append(result, s)
	}
	if sortSeries {
		slices.SortFunc(result, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}
	return newSeriesSet(result)
}

func (p *promQuerier) querySeriesSingleflight(ctx context.Context, samplePoints bool, params metricSelectParams) (_ metricSelectResult, rerr error) {
	if p.disableMetricOffloading {
		samplePoints = false
	}
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.querySeriesSingleflight",
		trace.WithAttributes(
			xattribute.StringerSlice("promql.matchers", params.Matchers),
			attribute.Int64("promql.step", params.Step.Milliseconds()),
			attribute.Int64("promql.start", params.Start.UnixMilli()),
			attribute.Int64("promql.end", params.End.UnixMilli()),
			attribute.Int64("promql.range", params.Range.Milliseconds()),
			attribute.Int64("promql.lookback_delta", params.LookbackDelta.Milliseconds()),
			attribute.String("promql.function", params.Function),
			attribute.Bool("promql.select_timestamp", params.SelectTimestamp),
			attribute.Bool("promql.group_by", params.GroupBy),
			attribute.StringSlice("promql.grouping", params.Grouping),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		parentSpan = span
		parentLink = trace.LinkFromContext(ctx)
		hash       = params.Hash(samplePoints)
	)
	resultCh := p.metricsSg.DoChanContext(ctx, hash, func(ctx context.Context) (_ metricSelectResult, rerr error) {
		ctx, span := p.tracer.Start(ctx, "chstorage.metrics.singelflight.querySeries",
			trace.WithLinks(parentLink),
		)
		defer func() {
			if rerr != nil {
				span.RecordError(rerr)
			}
			span.End()
		}()
		parentSpan.AddLink(trace.LinkFromContext(ctx))
		return p.querySeries(ctx, samplePoints, params)
	})

	select {
	case <-ctx.Done():
		return metricSelectResult{}, ctx.Err()
	case r := <-resultCh:
		result, shared, err := r.Val, r.Shared, r.Err

		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Query did not complete, probably stuck.
			span.AddEvent("retry_query")
			// Try again without singleflight.
			result, err = p.querySeries(ctx, samplePoints, params)
			shared = false
		}
		if err != nil {
			return result, err
		}

		span.AddEvent("got_series_set", trace.WithAttributes(
			attribute.Bool("chstorage.shared_result", shared),
			attribute.Int("chstorage.point_series", len(result.points)),
			attribute.Int("chstorage.exp_hist_series", len(result.expHist)),
		))
		return result, nil
	}
}

// ErrMetricsTooManySeries whether if query requested more timeseries than allowed.
var ErrMetricsTooManySeries = errors.New("too many timeseries requested")

func (p *promQuerier) querySeries(ctx context.Context, samplePoints bool, params metricSelectParams) (result metricSelectResult, _ error) {
	span := trace.SpanFromContext(ctx)

	timeseries, err := p.queryTimeseries(ctx, params.Start, params.End, [][]*labels.Matcher{params.Matchers})
	if err != nil {
		return result, errors.Wrap(err, "query timeseries hashes")
	}

	if len(timeseries) == 0 {
		// No data.
		trace.SpanFromContext(ctx).AddEvent("chstorage.no_timeseries_selected")
		return result, nil
	}
	if p.timeseriesLimit > 0 && len(timeseries) > p.timeseriesLimit {
		trace.SpanFromContext(ctx).AddEvent("chstorage.too_many_timeseries")
		return result, errors.Wrapf(ErrMetricsTooManySeries, "%d > %d series requested", len(timeseries), p.timeseriesLimit)
	}
	if samplePoints && !p.disableRateOffloading {
		if kind, ok := funcNameToRateKind(params.Function); ok {
			span.AddEvent("chstorage.use_rate_offloading", trace.WithAttributes(
				attribute.String("promql.function", params.Function),
			))

			var (
				points []*series[pointData]
				err    error
			)
			if p.metricsCache != nil {
				points, err = p.queryRatePointsCached(ctx, params.Start, params.End, params.Step, params.Range, params.Offset, timeseries, kind)
			} else {
				points, err = p.queryRatePoints(ctx, params.Start, params.End, params.Step, params.Range, params.Offset, timeseries, kind)
			}
			if err != nil {
				return result, errors.Wrap(err, "query rate points")
			}
			result.points = points
			return result, nil
		}
	}

	step := params.Step
	if r := params.LookbackDelta; r != 0 && r < step {
		step = r
	}
	if r := params.Range; r != 0 && r < step {
		step = r
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		if s, ok := canUseSampledPoints(step, params.Function); samplePoints && ok {
			span.AddEvent("chstorage.use_sampled_points", trace.WithAttributes(
				attribute.Int64("chstorage.window_step", step.Milliseconds()),
			))

			var (
				points []*series[pointData]
				err    error
			)
			if p.metricsCache != nil {
				points, err = p.querySampledPointsCached(ctx, s, params.GroupBy, params.Grouping, params.Start, params.End, step, timeseries)
			} else {
				points, err = p.querySampledPoints(ctx, s, params.GroupBy, params.Grouping, params.Start, params.End, step, timeseries)
			}
			if err != nil {
				return errors.Wrap(err, "query sampled points")
			}
			result.points = points
		} else {
			var (
				points []*series[pointData]
				err    error
			)
			if p.metricsCache != nil {
				points, err = p.queryPointsCached(ctx, p.tables.Points, params.Start, params.End, timeseries)
			} else {
				var set map[[16]byte]*series[pointData]
				set, err = p.queryPoints(ctx, p.tables.Points, params.Start, params.End, timeseries)
				if err == nil {
					points = make([]*series[pointData], 0, len(set))
					for _, s := range set {
						points = append(points, s)
					}
				}
			}
			if err != nil {
				return errors.Wrap(err, "query points")
			}
			result.points = points
		}

		return nil
	})
	if !params.SelectTimestamp {
		grp.Go(func() error {
			ctx := grpCtx

			expHist, err := p.queryExpHistograms(ctx, p.tables.ExpHistograms, params.Start, params.End, timeseries)
			if err != nil {
				return errors.Wrap(err, "query exponential histograms")
			}
			result.expHist = expHist

			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return result, err
	}

	return result, nil
}

func (p *promQuerier) queryPointsCached(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ []*series[pointData], rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryPointsCached",
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

	fetch := func(ctx context.Context, fetchStart, fetchEnd time.Time) (map[[16]byte]*series[pointData], error) {
		return p.queryPointsFunc(ctx, table, fetchStart, fetchEnd, timeseries)
	}
	resultMap, err := p.fetchAndMergeCache(ctx, span, start, end, 0, "", timeseries, fetch)
	if err != nil {
		return nil, err
	}

	result := make([]*series[pointData], 0, len(resultMap))
	for _, s := range resultMap {
		result = append(result, s)
	}

	totalPoints := 0
	for _, s := range result {
		totalPoints += len(s.ts)
	}
	span.AddEvent("chstorage.merged_result", trace.WithAttributes(
		attribute.Int("chstorage.merged_series", len(result)),
		attribute.Int("chstorage.merged_points", totalPoints),
	))

	return result, nil
}

func (p *promQuerier) querySampledPointsCached(
	ctx context.Context,
	sampler pointsSampler,
	on bool, groupBy []string,
	start, end time.Time,
	step time.Duration,
	timeseries map[[16]byte]labels.Labels,
) (_ []*series[pointData], rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.querySampledPointsCached",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	fetch := func(ctx context.Context, fetchStart, fetchEnd time.Time) (map[[16]byte]*series[pointData], error) {
		return p.querySampledPointsPerSeriesFunc(ctx, sampler, fetchStart, fetchEnd, step, timeseries)
	}
	resultMap, err := p.fetchAndMergeCache(ctx, span, start, end, step, sampler.fn, timeseries, fetch)
	if err != nil {
		return nil, err
	}

	if sampler.noClientGrouping || (!on && len(groupBy) == 0) {
		result := make([]*series[pointData], 0, len(resultMap))
		for _, s := range resultMap {
			result = append(result, s)
		}
		totalPoints := 0
		for _, s := range result {
			totalPoints += len(s.ts)
		}
		span.AddEvent("chstorage.merged_result", trace.WithAttributes(
			attribute.Int("chstorage.merged_series", len(result)),
			attribute.Int("chstorage.merged_points", totalPoints),
		))
		return result, nil
	}

	result := p.aggregateSampledPoints(ctx, resultMap, on, groupBy, sampler, step)
	totalPoints := 0
	for _, s := range result {
		totalPoints += len(s.ts)
	}
	span.AddEvent("chstorage.merged_aggregated_result", trace.WithAttributes(
		attribute.Int("chstorage.merged_series", len(result)),
		attribute.Int("chstorage.merged_points", totalPoints),
	))
	return result, nil
}

// upsertCache writes fetched points and watermark for one series into the cache.
// If onlyIfExists is true and the entry is not yet in the cache, the call is a no-op:
// this prevents a large query from evicting warm entries belonging to other series.
func (p *promQuerier) upsertCache(ctx context.Context, hash [16]byte, step time.Duration, fn string, fetchFrom int64, ts []int64, vals []float64, untilMs int64, onlyIfExists bool) {
	key := metricscache.Key{
		Hash: hash,
		Step: step.Milliseconds(),
		Fn:   fn,
	}
	entry, ok := p.metricsCache.Get(key)
	if !ok {
		if onlyIfExists {
			p.metricsCache.Stats.SkippedInserts.Add(ctx, 1, cacheTypeOpt(fn, step))
			return
		}
		entry = metricscache.NewEntry()
	} else {
		// If the new fetch range does not adjoin the cached range there is an
		// unfetched gap.  Reusing the existing entry would cause MarkFetched to
		// extend the watermark over that gap, making the cache falsely claim
		// coverage for data it never retrieved.  Start fresh so that the next
		// query for the full range is correctly treated as a miss.
		//
		// Two gap directions:
		//   forward: new fetch starts well after the cached maximum
		//            (gap in [oldMaxTS, fetchFrom))
		//   backward: new fetch ends well before the cached minimum
		//             (gap in [untilMs, oldMinTS))
		oldMinTS, oldMaxTS := entry.Watermarks()
		if oldMaxTS != math.MinInt64 {
			stepMs := step.Milliseconds()
			forwardGap := fetchFrom > nextAlignedFetch(oldMaxTS, stepMs)
			backwardGap := untilMs < oldMinTS-max(1, stepMs)
			if forwardGap || backwardGap {
				entry = metricscache.NewEntry()
			}
		}
	}

	entry.Append(ts, vals, untilMs)
	// Always advance the watermark so series with no data in the fetched range
	// are treated as cache hits on the next query (skipping the ClickHouse round-trip).
	entry.MarkFetched(fetchFrom, untilMs)
	p.metricsCache.Set(key, entry)
}

var (
	cacheTypeRaw        = metric.WithAttributes(attribute.String("cache_type", "raw"))
	cacheTypeAggregated = metric.WithAttributes(attribute.String("cache_type", "aggregated"))
	cacheTypeRate       = metric.WithAttributes(attribute.String("cache_type", "rate"))
)

func cacheTypeOpt(fn string, step time.Duration) metric.MeasurementOption {
	if step == 0 {
		return cacheTypeRaw
	}
	if _, ok := funcNameToRateKind(fn); ok {
		return cacheTypeRate
	}
	return cacheTypeAggregated
}

type cacheStats struct {
	hits        int
	partialHits int
	misses      int
}

const uncachedWatermark = -1

// nextAlignedFetch returns the earliest timestamp to fetch after a cache watermark.
// For step-aligned queries (stepMs > 0) it rounds up to the next step boundary so
// that the first fetched point is never shifted by 1 ms relative to the Prometheus
// step grid.  For raw-point queries (stepMs == 0) it returns watermark+1.
func nextAlignedFetch(watermarkMs, stepMs int64) int64 {
	if stepMs <= 0 {
		return watermarkMs + 1
	}
	return (watermarkMs/stepMs + 1) * stepMs
}

func computeFetchRange(
	ctx context.Context,
	cache *metricscache.Cache,
	step time.Duration,
	fn string,
	start int64,
	timeseries map[[16]byte]labels.Labels,
) (watermarks map[[16]byte]int64, globalFetchFrom int64, stats cacheStats) {
	watermarks = make(map[[16]byte]int64, len(timeseries))
	var (
		hasAnyMiss = false
		minHitTS   int64
	)
	stepMs := step.Milliseconds()
	for hash := range timeseries {
		watermarks[hash] = uncachedWatermark
		key := metricscache.Key{
			Hash: hash,
			Step: stepMs,
			Fn:   fn,
		}
		entry, ok := cache.Get(key)
		if ok {
			entryMinTS, entryMaxTS := entry.Watermarks()

			if entryMinTS <= start && entryMaxTS >= start {
				stats.hits++
				watermarks[hash] = entryMaxTS
				if stats.hits == 1 || entryMaxTS < minHitTS {
					minHitTS = entryMaxTS
				}
				continue
			}

			stats.partialHits++
		}
		stats.misses++
		hasAnyMiss = true
	}

	if hasAnyMiss || stats.hits == 0 {
		globalFetchFrom = start
	} else {
		// All series are hits. Fetch from the next step boundary after minHitTS so
		// that step-aligned queries don't produce a 1ms-shifted first point.
		globalFetchFrom = nextAlignedFetch(minHitTS, stepMs)
	}

	opt := cacheTypeOpt(fn, step)
	cache.Stats.SeriesHits.Add(ctx, int64(stats.hits), opt)
	cache.Stats.SeriesPartialHits.Add(ctx, int64(stats.partialHits), opt)
	cache.Stats.SeriesMisses.Add(ctx, int64(stats.misses), opt)

	return watermarks, globalFetchFrom, stats
}

func canUseSampledPoints(stepDuration time.Duration, fn string) (pointsSampler, bool) {
	if stepDuration < time.Second {
		return pointsSampler{}, false
	}
	switch fn {
	case "":
		return pointsSampler{
			pointExpr:  chsql.AnyLast,
			needPoints: true,
			agg:        anyLastAggregator,
			fn:         "",
		}, true
	case "sum", "sum_over_time":
		return pointsSampler{
			pointExpr:  chsql.Sum,
			needPoints: true,
			agg:        sumAggregator,
			fn:         "sum",
		}, true
	case "avg", "avg_over_time":
		return pointsSampler{
			pointExpr:  chsql.Avg,
			needPoints: true,
			agg:        avgAggregator,
			fn:         "avg",
		}, true
	case "min", "min_over_time":
		return pointsSampler{
			pointExpr:  chsql.Min,
			needPoints: true,
			agg:        minAggregator,
			fn:         "min",
		}, true
	case "max", "max_over_time":
		return pointsSampler{
			pointExpr:  chsql.Max,
			needPoints: true,
			agg:        maxAggregator,
			fn:         "max",
		}, true
	case "count":
		// count (vector aggregation) counts series, not values, so PromQL must
		// see all individual series. We still benefit from the sampled path
		// (fewer raw points fetched per window), but skip client-side grouping.
		return pointsSampler{
			pointExpr: func(chsql.Expr) chsql.Expr {
				return chsql.Count()
			},
			needPoints:       true,
			agg:              sumAggregator,
			fn:               "count",
			noClientGrouping: true,
		}, true
	case "count_over_time":
		return pointsSampler{
			pointExpr: func(chsql.Expr) chsql.Expr {
				return chsql.Count()
			},
			needPoints: true,
			agg:        sumAggregator,
			fn:         "count_over_time",
		}, true
	default:
		return pointsSampler{}, false
	}
}

// samplerAggregator defines how per-series pre-aggregated values are combined
// during client-side grouping in aggregateSampledPoints.
type samplerAggregator struct {
	// combine merges a new per-series value v into the running accumulator acc.
	// Called only after the first value (which seeds acc directly).
	combine func(acc, v float64) float64
	// finalize converts the accumulated value to the final result given the
	// number of series that contributed to it.
	finalize func(acc float64, n int) float64
}

var (
	sumAggregator     = samplerAggregator{func(a, b float64) float64 { return a + b }, func(a float64, _ int) float64 { return a }}
	avgAggregator     = samplerAggregator{func(a, b float64) float64 { return a + b }, func(a float64, n int) float64 { return a / float64(n) }}
	minAggregator     = samplerAggregator{math.Min, func(a float64, _ int) float64 { return a }}
	maxAggregator     = samplerAggregator{math.Max, func(a float64, _ int) float64 { return a }}
	anyLastAggregator = samplerAggregator{func(_, b float64) float64 { return b }, func(a float64, _ int) float64 { return a }}
)

type pointsSampler struct {
	pointExpr  func(column chsql.Expr) chsql.Expr
	needPoints bool
	agg        samplerAggregator
	// fn is the canonical function name used as part of the cache key to prevent
	// different aggregators from sharing a cache entry for the same (hash, step).
	fn string
	// noClientGrouping disables the client-side aggregateSampledPoints step so
	// all per-series results are returned as-is for PromQL to aggregate.
	// Required for "count": PromQL counts the number of series, not their values,
	// so pre-collapsing series would make PromQL count 1 instead of N.
	noClientGrouping bool
}

func (p *promQuerier) queryPoints(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ map[[16]byte]*series[pointData], rerr error) {
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
			{Name: "hash", Data: &inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			var (
				lastHash   [16]byte
				lastSeries *series[pointData]
			)
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					hash      = c.hash.Row(i)
					value     = c.value.Row(i)
					timestamp = c.timestamp.Row(i)
				)
				var s *series[pointData]
				if lastSeries != nil && hash == lastHash {
					s = lastSeries
				} else {
					var ok bool
					s, ok = set[hash]
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
					lastHash = hash
					lastSeries = s
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

	return set, nil
}

type groupMapping struct {
	// groups maps the unique group hash to the canonical set of labels that define that group.
	// This set of labels is the result of applying the grouping criteria (e.g., 'by job, instance').
	groups map[uint64]labels.Labels

	// inputHash contains a hash of timeseries.
	inputHash     proto.ColFixedStr16
	inputGrouping *proto.ColLowCardinality[uint64]
}

// makeGroupMapping generates a mapping to group timeseries by labels.
func makeGroupMapping(on bool, groupBy []string, timeseries map[[16]byte]labels.Labels) (m groupMapping) {
	m = groupMapping{
		groups:        map[uint64]labels.Labels{},
		inputGrouping: new(proto.ColUInt64).LowCardinality(),
	}

	var i uint64
	for tsHash, labels := range timeseries {
		var groupHash uint64
		if len(groupBy) > 0 {
			labels = labels.MatchLabels(on, groupBy...)
			groupHash = labels.Hash()
		} else {
			groupHash = i
			i++
		}
		m.groups[groupHash] = labels

		m.inputHash.Append(tsHash)
		m.inputGrouping.Append(groupHash)
	}
	return m
}

// querySampledPoints selects pre-sampled points from Clickhouse.
//
// Since PromQL engine selects only the few values in the vector for each step in some cases, we don't actually need to query every sample.
// Instead, we select only these few values within time window.
//
// It reduces overall memory usage and data transfer.
func (p *promQuerier) querySampledPoints(
	ctx context.Context,
	sampler pointsSampler,
	on bool, groupBy []string,
	start, end time.Time,
	step time.Duration,
	timeseries map[[16]byte]labels.Labels,
) (_ []*series[pointData], rerr error) {
	set, err := p.querySampledPointsPerSeries(ctx, sampler, start, end, step, timeseries)
	if err != nil {
		return nil, err
	}

	if sampler.noClientGrouping || (!on && len(groupBy) == 0) {
		result := make([]*series[pointData], 0, len(set))
		for _, s := range set {
			result = append(result, s)
		}
		return result, nil
	}

	return p.aggregateSampledPoints(ctx, set, on, groupBy, sampler, step), nil
}

func (p *promQuerier) querySampledPointsPerSeries(
	ctx context.Context,
	sampler pointsSampler,
	start, end time.Time,
	step time.Duration,
	timeseries map[[16]byte]labels.Labels,
) (_ map[[16]byte]*series[pointData], rerr error) {
	table := p.tables.Points

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.querySampledPointsPerSeries",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Bool("chstorage.sampler.need_points", sampler.needPoints),
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
		inputTable = "timeseries_hashes"
		// We query per-series, so we use empty groupBy.
		mapping = makeGroupMapping(false, nil, timeseries)

		groupCol     = new(proto.ColUInt64).LowCardinality()
		timestampCol = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		valueCol     proto.ColFloat64

		results = []chsql.ResultColumn{
			{
				Name: "group",
				Expr: chsql.PrefixedIdent(inputTable, "grouping"),
				Data: groupCol,
			},
			{
				Name: "step_ts",
				Expr: chsql.Ident("window_end"),
				Data: timestampCol,
			},
		}
	)
	if sampler.needPoints {
		expr := chsql.ToFloat64(sampler.pointExpr(chsql.Ident("value")))
		results = append(results, chsql.ResultColumn{
			Name: "agg",
			Expr: expr,
			Data: &valueCol,
		})

		printer := chsql.GetPrinter()
		if err := printer.WriteExpr(expr); err == nil {
			span.SetAttributes(attribute.String("chstorage.sampler.agg_expr", printer.String()))
		}
		chsql.PutPrinter(printer)
	}

	var (
		datetime  = chsql.ToDateTime(chsql.Ident("timestamp"))
		hashQuery = chsql.Select(inputTable, chsql.Column("hash", nil))
		joinExpr  = chsql.Eq(
			chsql.PrefixedIdent(inputTable, "hash"),
			chsql.PrefixedIdent(table, "hash"),
		)

		query = chsql.Select(table, results...).
			With("step", chsql.Interval(step)).
			With("window_start", chsql.ToDateTime64(chsql.TumbleStart(datetime, step), proto.PrecisionMilli)).
			With("window_end", chsql.ToDateTime64(chsql.TumbleEnd(datetime, step), proto.PrecisionMilli)).
			InnerJoin(inputTable, "", joinExpr).
			Where(
				chsql.InTimeRange("timestamp", start, end, proto.PrecisionMilli),
				chsql.In(
					chsql.Ident("hash"),
					chsql.SubQuery(hashQuery),
				),
			).
			GroupBy(
				chsql.Ident("group"),
				chsql.Ident("window_start"),
				chsql.Ident("window_end"),
			).
			Order(chsql.Ident("group"), chsql.Asc).
			Order(chsql.Ident("window_end"), chsql.Asc)
	)

	// Since we used empty groupBy, mapping.groups[i] is the labels of i-th timeseries.
	// We need to map i back to its hash.
	// We'll reconstruct the hash map from the inputHash column.
	hashes := make(map[uint64][16]byte, len(timeseries))
	for i := 0; i < mapping.inputHash.Rows(); i++ {
		hashes[uint64(i)] = mapping.inputHash.Row(i)
	}

	var (
		set         = map[[16]byte]*series[pointData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         query,
		ExternalTable: inputTable,
		ExternalData: []proto.InputColumn{
			{Name: "hash", Data: &mapping.inputHash},
			{Name: "grouping", Data: mapping.inputGrouping},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			var (
				lastGroup  uint64
				hasGroup   bool
				lastSeries *series[pointData]
			)
			for i := 0; i < timestampCol.Rows(); i++ {
				var (
					group     = groupCol.Row(i)
					timestamp = timestampCol.Row(i)
				)
				var s *series[pointData]
				if hasGroup && group == lastGroup {
					s = lastSeries
				} else {
					hash, ok := hashes[group]
					if !ok {
						return errors.Errorf("can't find hash for requested group %d", group)
					}

					var ok2 bool
					s, ok2 = set[hash]
					if !ok2 {
						lb, ok := mapping.groups[group]
						if !ok {
							return errors.Errorf("can't find labels for requested series %d", group)
						}
						s = &series[pointData]{
							labels: lb,
						}
						set[hash] = s
					}
					lastGroup = group
					hasGroup = true
					lastSeries = s
				}

				if sampler.needPoints {
					s.data.values = append(s.data.values, valueCol.Row(i))
				} else {
					s.data.values = append(s.data.values, 1)
				}
				s.ts = append(s.ts, timestamp.UnixMilli())

				totalPoints++
			}
			return nil
		},

		Type:   "QuerySampledPoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, errors.Wrap(err, "query sampled points per series")
	}
	span.AddEvent("points_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_points", totalPoints),
	))

	return set, nil
}

func (p *promQuerier) aggregateSampledPoints(
	ctx context.Context,
	set map[[16]byte]*series[pointData],
	on bool, groupBy []string,
	sampler pointsSampler,
	step time.Duration,
) []*series[pointData] {
	_, span := p.tracer.Start(ctx, "chstorage.metrics.aggregateSampledPoints",
		trace.WithAttributes(
			attribute.Bool("chstorage.group.on", on),
			attribute.StringSlice("chstorage.group.by", groupBy),
			attribute.Int("chstorage.total_series", len(set)),
		),
	)
	defer span.End()

	if len(set) == 0 {
		return nil
	}

	stepMs := step.Milliseconds()
	span.SetAttributes(attribute.Int64("chstorage.aggregate.step_ms", stepMs))
	// TumbleEnd(ts, step) = (floor(ts/step) + 1) * step
	// The timestamps from ClickHouse are aligned to step boundaries.
	// We can use a slice if we know the range.
	var (
		minTS   int64 = math.MaxInt64
		maxTS   int64 = math.MinInt64
		aligned       = true
	)
	for _, s := range set {
		for _, t := range s.ts {
			if t < minTS {
				minTS = t
			}
			if t > maxTS {
				maxTS = t
			}
			if t%stepMs != 0 {
				aligned = false
			}
		}
	}

	if minTS == math.MaxInt64 {
		return nil
	}

	// Safety check for crazy ranges and misaligned timestamps.
	numSteps := int((maxTS-minTS)/stepMs) + 1
	if !aligned || numSteps > 100_000 {
		// Fallback to map-based aggregation if range is too large or timestamps are misaligned.
		return p.aggregateSampledPointsMap(ctx, set, on, groupBy, sampler)
	}

	type groupKey struct {
		hash uint64
	}
	type groupData struct {
		labels labels.Labels
		values []float64
		counts []int
	}
	groups := map[groupKey]*groupData{}

	agg := sampler.agg
	for _, s := range set {
		lb := s.labels
		if len(groupBy) > 0 {
			lb = s.labels.MatchLabels(on, groupBy...)
		}
		key := groupKey{hash: lb.Hash()}
		g, ok := groups[key]
		if !ok {
			g = &groupData{
				labels: lb,
				values: make([]float64, numSteps),
				counts: make([]int, numSteps),
			}
			groups[key] = g
		}

		for i, ts := range s.ts {
			idx := (ts - minTS) / stepMs
			if idx < 0 || idx >= int64(numSteps) {
				continue
			}
			v := s.data.values[i]
			if g.counts[idx] == 0 {
				g.values[idx] = v
			} else {
				g.values[idx] = agg.combine(g.values[idx], v)
			}
			g.counts[idx]++
		}
	}

	result := make([]*series[pointData], 0, len(groups))
	for _, g := range groups {
		s := &series[pointData]{
			labels: g.labels,
		}
		for i, count := range g.counts {
			if count == 0 {
				continue
			}
			s.ts = append(s.ts, minTS+int64(i)*stepMs)
			s.data.values = append(s.data.values, agg.finalize(g.values[i], count))
		}
		result = append(result, s)
	}
	span.SetAttributes(attribute.Int("chstorage.grouped_series", len(result)))
	return result
}

func (p *promQuerier) aggregateSampledPointsMap(
	_ context.Context,
	set map[[16]byte]*series[pointData],
	on bool, groupBy []string,
	sampler pointsSampler,
) []*series[pointData] {
	// Group series by requested labels.
	type groupKey struct {
		hash uint64
	}
	type groupData struct {
		labels labels.Labels
		// ts -> accumulated value
		points map[int64]float64
		// ts -> number of series that contributed (for finalize)
		counts map[int64]int
	}
	groups := map[groupKey]*groupData{}

	agg := sampler.agg
	for _, s := range set {
		lb := s.labels
		if len(groupBy) > 0 {
			lb = s.labels.MatchLabels(on, groupBy...)
		}
		key := groupKey{hash: lb.Hash()}
		g, ok := groups[key]
		if !ok {
			g = &groupData{
				labels: lb,
				points: map[int64]float64{},
				counts: map[int64]int{},
			}
			groups[key] = g
		}

		for i, ts := range s.ts {
			v := s.data.values[i]
			if n := g.counts[ts]; n == 0 {
				// First value seeds the accumulator directly.
				g.points[ts] = v
			} else {
				g.points[ts] = agg.combine(g.points[ts], v)
			}
			g.counts[ts]++
		}
	}

	result := make([]*series[pointData], 0, len(groups))
	for _, g := range groups {
		s := &series[pointData]{
			labels: g.labels,
		}
		// Sort points by timestamp.
		timestamps := make([]int64, 0, len(g.points))
		for ts := range g.points {
			timestamps = append(timestamps, ts)
		}
		slices.Sort(timestamps)

		for _, ts := range timestamps {
			s.ts = append(s.ts, ts)
			s.data.values = append(s.data.values, agg.finalize(g.points[ts], g.counts[ts]))
		}
		result = append(result, s)
	}
	return result
}

func (p *promQuerier) queryExpHistograms(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ []*series[expHistData], rerr error) {
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
			{Name: "hash", Data: &inputData},
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

	result := make([]*series[expHistData], 0, len(set))
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

func (s *seriesSet[S]) Clone() *seriesSet[S] {
	return newSeriesSet(s.set)
}

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
