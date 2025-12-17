package chstorage

import (
	"context"
	"encoding/hex"
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
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

var _ storage.Queryable = (*Querier)(nil)

// Querier returns a new metrics [storage.Querier].
func (q *Querier) Querier(mint, maxt int64) (storage.Querier, error) {
	return q.metricsQuerier(mint, maxt), nil
}

type promQuerier struct {
	mint time.Time
	maxt time.Time

	tables     Tables
	labelLimit int

	queryTimeseries queryMetricsTimeseriesFunc
	metricsSg       *singleflight.Group[xxh3.Uint128, metricSelectResult]
	do              func(ctx context.Context, s selectQuery) error

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
	return &promQuerier{
		mint: minTime,
		maxt: maxTime,

		tables:          q.tables,
		labelLimit:      q.labelLimit,
		metricsSg:       q.metricsSg,
		queryTimeseries: q.timeseries.Query,
		do:              q.do,

		tracer: q.tracer,
	}
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

	r, err := p.querySeriesSingleflight(ctx, false, metricSelectParams{
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

	Function string

	SelectTimestamp bool
	GroupBy         bool
	Grouping        []string
}

func (p *metricSelectParams) Hash() xxh3.Uint128 {
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
	writeString(h, p.Function)
	writeBool(h, p.SelectTimestamp)
	writeBool(h, p.GroupBy)
	writeStrings(h, p.Grouping)
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
		hash       = params.Hash()
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

func (p *promQuerier) querySeries(ctx context.Context, samplePoints bool, params metricSelectParams) (result metricSelectResult, _ error) {
	span := trace.SpanFromContext(ctx)

	timeseries, err := p.queryTimeseries(ctx, [][]*labels.Matcher{params.Matchers})
	if err != nil {
		return result, errors.Wrap(err, "query timeseries hashes")
	}

	if len(timeseries) == 0 {
		// No data.
		trace.SpanFromContext(ctx).AddEvent("chstorage.no_timeseries_selected")
		return result, nil
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

		if samplePoints && canUseSampledPoints(step, params.Function) {
			span.AddEvent("chstorage.use_sampled_points", trace.WithAttributes(
				attribute.Int64("chstorage.window_step", step.Milliseconds()),
			))

			points, err := p.querySampledPoints(ctx, params.GroupBy, params.Grouping, params.Start, params.End, step, timeseries)
			if err != nil {
				return errors.Wrap(err, "query sampled points")
			}
			result.points = points
		} else {
			points, err := p.queryPoints(ctx, p.tables.Points, params.Start, params.End, timeseries)
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

func (p *promQuerier) queryPoints(ctx context.Context, table string, start, end time.Time, timeseries map[[16]byte]labels.Labels) (_ []*series[pointData], rerr error) {
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

	result := make([]*series[pointData], 0, len(set))
	for _, s := range set {
		result = append(result, s)
	}
	return result, nil
}

type groupMapping struct {
	// groups maps the unique group hash to the canonical set of labels that define that group.
	// This set of labels is the result of applying the grouping criteria (e.g., 'by job, instance').
	groups map[uint64]labels.Labels

	// inputHash contains a hash of timeseries.
	inputHash proto.ColFixedStr16
	// groupingExpr contains a map literal mapping timeseries to a corresponding group.
	groupingExpr chsql.Expr
}

// makeGroupMapping generates a mapping to group timeseries by labels.
func makeGroupMapping(on bool, groupBy []string, timeseries map[[16]byte]labels.Labels) (m groupMapping) {
	m = groupMapping{
		groups: map[uint64]labels.Labels{},
	}

	var (
		kv = make([]chsql.Expr, 0, len(timeseries))
		i  uint64
	)
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

		key := chsql.String(hex.EncodeToString(tsHash[:]))
		value := chsql.ToUInt64(chsql.Integer(groupHash))
		kv = append(kv,
			chsql.Unhex(key),
			value,
		)
	}
	m.groupingExpr = chsql.Map(kv...)
	return m
}

// querySampledPoints selects pre-sampled points from Clickhouse.
//
// Since PromQL engine selects only the last value in the vector for each step in some cases, we don't actually need to query every sample.
// Instead, we select only last one within time window.
//
// It reduces overall memory usage and data transfer.
func (p *promQuerier) querySampledPoints(ctx context.Context, on bool, groupBy []string, start, end time.Time, step time.Duration, timeseries map[[16]byte]labels.Labels) (_ []*series[pointData], rerr error) {
	table := p.tables.Points

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.querySampledPoints",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Bool("chstorage.group.on", on),
			attribute.StringSlice("chstorage.group.by", groupBy),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	mapping := makeGroupMapping(on, groupBy, timeseries)
	var (
		group     = new(proto.ColUInt64).LowCardinality()
		timestamp = new(proto.ColDateTime)
		value     proto.ColFloat64

		datetime = chsql.ToDateTime(chsql.Ident("timestamp"))
		query    = chsql.Select(table,
			chsql.ResultColumn{
				Name: "group",
				Expr: chsql.ArrayElement(
					chsql.Ident("grouping"),
					chsql.Ident("hash"),
				),
				Data: group,
			},
			chsql.ResultColumn{
				Name: "step_ts",
				Expr: chsql.Ident("window_end"),
				Data: timestamp,
			},
			chsql.ResultColumn{
				Name: "agg",
				Expr: chsql.LastValue(chsql.Ident("value")),
				Data: &value,
			},
		).
			With("step", chsql.Interval(step)).
			With("window_start", chsql.TumbleStart(datetime, step)).
			With("window_end", chsql.TumbleEnd(datetime, step)).
			With("grouping", mapping.groupingExpr).
			Where(
				chsql.In(
					chsql.Ident("hash"),
					chsql.Ident("timeseries_hashes"),
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
	if !start.IsZero() {
		query.Where(chsql.Gte(
			chsql.Ident("timestamp"),
			chsql.DateTime(start),
		))
	}
	if !end.IsZero() {
		query.Where(chsql.Lte(
			chsql.Ident("timestamp"),
			chsql.DateTime(end),
		))
	}

	var (
		set         = map[uint64]*series[pointData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         query,
		ExternalTable: "timeseries_hashes",
		ExternalData: []proto.InputColumn{
			{Name: "hash", Data: &mapping.inputHash},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < timestamp.Rows(); i++ {
				var (
					group     = group.Row(i)
					value     = value.Row(i)
					timestamp = timestamp.Row(i)
				)
				s, ok := set[group]
				if !ok {
					lb, ok := mapping.groups[group]
					if !ok {
						zctx.From(ctx).Error("Can't find labels for requested series")
						continue
					}
					s = &series[pointData]{
						labels: lb,
					}
					set[group] = s
				}

				s.data.values = append(s.data.values, value)
				s.ts = append(s.ts, timestamp.UnixMilli())

				totalPoints++
			}
			return nil
		},

		Type:   "QuerySampledPoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("points_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_points", totalPoints),
	))

	result := make([]*series[pointData], 0, len(set))
	for _, s := range set {
		result = append(result, s)
	}
	return result, nil
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
