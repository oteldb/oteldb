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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/otelstorage"
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

		ch:              q.ch,
		tables:          q.tables,
		labelLimit:      q.labelLimit,
		getLabelMapping: q.getMetricsLabelMapping,
		do:              q.do,

		clickhouseRequestHistogram: q.clickhouseRequestHistogram,
		tracer:                     q.tracer,
	}, nil
}

type promQuerier struct {
	mint time.Time
	maxt time.Time

	ch              ClickHouseClient
	tables          Tables
	labelLimit      int
	getLabelMapping func(context.Context, []string) (metricsLabelMapping, error)
	do              func(ctx context.Context, s selectQuery) error

	clickhouseRequestHistogram metric.Float64Histogram
	tracer                     trace.Tracer
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

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifetime of the querier.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (p *promQuerier) LabelValues(ctx context.Context, labelName string, hints *storage.LabelHints, matchers ...*labels.Matcher) (result []string, _ annotations.Annotations, rerr error) {
	if hints == nil {
		hints = &storage.LabelHints{}
	}
	labelName = DecodeUnicodeLabel(labelName)
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.LabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.label", labelName),
			attribute.Int("chstorage.hints.limits", hints.Limit),
			xattribute.StringerSlice("chstorage.matchers", matchers),
			xattribute.UnixNano("chstorage.mint", p.mint),
			xattribute.UnixNano("chstorage.maxt", p.maxt),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			span.AddEvent("values_fetched", trace.WithAttributes(
				attribute.Int("chstorage.total_values", len(result)),
			))
		}
		span.End()
	}()

	matchesOtherLabels := slices.ContainsFunc(matchers, func(m *labels.Matcher) bool {
		return labelName != m.Name
	})
	if matchesOtherLabels {
		r, err := p.getMatchingLabelValues(ctx, labelName, matchers)
		if err != nil {
			return nil, nil, err
		}
		return r, nil, nil
	}

	r, err := p.getLabelValues(ctx, labelName, matchers...)
	if err != nil {
		return nil, nil, err
	}
	return r, nil, nil
}

func (p *promQuerier) getLabelValues(ctx context.Context, labelName string, matchers ...*labels.Matcher) (result []string, rerr error) {
	table := p.tables.Labels

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.getLabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.label", labelName),
			xattribute.StringerSlice("chstorage.matchers", matchers),

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
		value       proto.ColStr
		valueColumn = "value"
	)

	query := chsql.Select(table, chsql.Column(valueColumn, &value)).
		Distinct(true).
		Where(chsql.ColumnEq("name", labelName))
	for _, m := range matchers {
		if m.Name != labelName {
			return nil, errors.Errorf("unexpected label matcher %s (label must be %q)", m, labelName)
		}

		expr, err := promQLLabelMatcher(
			[]chsql.Expr{chsql.Ident(valueColumn)},
			m.Type,
			m.Value,
		)
		if err != nil {
			return nil, err
		}
		query.Where(expr)
	}
	query.Order(chsql.Ident(valueColumn), chsql.Asc)

	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < value.Rows(); i++ {
				result = append(result, value.Row(i))
			}
			return nil
		},

		Type:   "getLabelValues",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (p *promQuerier) getMatchingLabelValues(ctx context.Context, labelName string, matchers []*labels.Matcher) (_ []string, rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.getMatchingLabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.label", labelName),
			xattribute.StringerSlice("chstorage.matchers", matchers),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	mlabels := make([]string, 0, len(matchers))
	for _, m := range matchers {
		mlabels = append(mlabels, m.Name)
	}
	mlabels = append(mlabels, labelName)
	zctx.From(ctx).Debug("Getting label mapping",
		zap.String("label", labelName),
		zap.Strings("labels", mlabels),
	)
	mapping, err := p.getLabelMapping(ctx, mlabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	query := func(ctx context.Context, table string) (result []string, rerr error) {
		var columnExpr chsql.Expr
		if labelName == labels.MetricName {
			columnExpr = chsql.Ident("name")
		} else {
			columnExpr = firstAttrSelector(labelName)
		}

		var (
			value proto.ColStr

			query = chsql.Select(table, chsql.ResultColumn{
				Name: "value",
				Expr: columnExpr,
				Data: &value,
			}).
				Distinct(true).
				Where(chsql.InTimeRange("timestamp", p.mint, p.maxt))
		)
		for _, m := range matchers {
			selectors := []chsql.Expr{
				chsql.Ident("name"),
			}
			if name := m.Name; name != labels.MetricName {
				selectors = mapping.Selectors(name)
			}

			expr, err := promQLLabelMatcher(selectors, m.Type, m.Value)
			if err != nil {
				return nil, err
			}
			query.Where(expr)
		}
		query.Limit(p.labelLimit)

		if err := p.do(ctx, selectQuery{
			Query: query,
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < value.Rows(); i++ {
					if v := value.Row(i); v != "" {
						result = append(result, v)
					}
				}
				return nil
			},

			Type:   "getMatchingLabelValues",
			Signal: "metrics",
			Table:  table,
		}); err != nil {
			return nil, err
		}
		span.AddEvent("values_fetched", trace.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.Int("chstorage.total_values", len(result)),
		))

		return result, nil
	}

	var (
		points   []string
		expHists []string
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.Points

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrapf(err, "query label values from %q", table)
		}
		points = result

		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.ExpHistograms

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrapf(err, "query label values from %q", table)
		}
		expHists = result

		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	points = append(points, expHists...)
	slices.Sort(points)
	// Remove duplicates.
	points = slices.Clip(points)

	return points, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
// If matchers are specified the returned result set is reduced
// to label names of metrics matching the matchers.
func (p *promQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) (result []string, _ annotations.Annotations, rerr error) {
	if hints == nil {
		hints = &storage.LabelHints{}
	}
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.LabelNames",
		trace.WithAttributes(
			xattribute.StringerSlice("chstorage.matchers", matchers),
			attribute.Int("chstorage.hints.limit", hints.Limit),
			xattribute.UnixNano("chstorage.mint", p.mint),
			xattribute.UnixNano("chstorage.maxt", p.maxt),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			span.AddEvent("names_fetched", trace.WithAttributes(
				attribute.Int("chstorage.total_names", len(result)),
			))
		}
		span.End()
	}()

	var err error
	if len(matchers) > 0 {
		result, err = p.getMatchingLabelNames(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}
		return result, nil, nil
	}

	result, err = p.getLabelNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

func (p *promQuerier) getLabelNames(ctx context.Context) (result []string, rerr error) {
	table := p.tables.Labels

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.getLabelNames",
		trace.WithAttributes(
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
		value = new(proto.ColStr).LowCardinality()
		query = chsql.Select(table, chsql.Column("name", value)).
			Distinct(true).
			Order(chsql.Ident("name"), chsql.Asc)
	)
	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < value.Rows(); i++ {
				result = append(result, value.Row(i))
			}
			return nil
		},

		Type:   "LabelNames",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (p *promQuerier) getMatchingLabelNames(ctx context.Context, matchers []*labels.Matcher) (_ []string, rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.getMatchingLabelNames",
		trace.WithAttributes(
			xattribute.StringerSlice("chstorage.matchers", matchers),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	mlabels := make([]string, 0, len(matchers))
	for _, m := range matchers {
		mlabels = append(mlabels, m.Name)
	}
	mapping, err := p.getLabelMapping(ctx, mlabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	query := func(ctx context.Context, table string) (result []string, rerr error) {
		var (
			value proto.ColStr

			query = chsql.Select(table, chsql.ResultColumn{
				Name: "values",
				Expr: chsql.ArrayJoin(
					chsql.ArrayConcat(
						attrKeys(colAttrs),
						attrKeys(colScope),
						attrKeys(colResource),
					),
				),
				Data: &value,
			}).
				Distinct(true).
				Where(chsql.InTimeRange("timestamp", p.mint, p.maxt))
		)
		for _, m := range matchers {
			selectors := []chsql.Expr{
				chsql.Ident("name"),
			}
			if name := m.Name; name != labels.MetricName {
				selectors = mapping.Selectors(name)
			}

			expr, err := promQLLabelMatcher(selectors, m.Type, m.Value)
			if err != nil {
				return nil, err
			}
			query.Where(expr)
		}
		query.Limit(p.labelLimit)

		if err := p.do(ctx, selectQuery{
			Query: query,
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < value.Rows(); i++ {
					result = append(result, value.Row(i))
				}
				return nil
			},

			Type:   "getMatchingLabelValues",
			Signal: "metrics",
			Table:  table,
		}); err != nil {
			return nil, err
		}
		span.AddEvent("labels_fetched", trace.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.Int("chstorage.total_values", len(result)),
		))

		return result, nil
	}

	var (
		points   []string
		expHists []string
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.Points

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrap(err, "query points series")
		}
		points = result

		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.ExpHistograms

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrap(err, "query exponential histogram series")
		}
		expHists = result

		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	points = append(points, expHists...)
	if len(points) > 0 {
		// Add `__name__` only if there is any matching series.
		points = append(points, labels.MetricName)
	}

	slices.Sort(points)
	// Remove duplicates.
	points = slices.Clip(points)

	return points, nil
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

type metricsLabelMapping struct {
	scope map[string]labelScope
}

func (m metricsLabelMapping) Selectors(key string) []chsql.Expr {
	name := key

	scopes := m.scope[key]
	if scopes == 0 {
		return []chsql.Expr{
			attrSelector(colAttrs, name),
			attrSelector(colScope, name),
			attrSelector(colResource, name),
		}
	}

	exprs := make([]chsql.Expr, 0, 3)
	for _, s := range []struct {
		flag   labelScope
		column string
	}{
		{labelScopeAttribute, colAttrs},
		{labelScopeInstrumentation, colScope},
		{labelScopeResource, colResource},
	} {
		if scopes&s.flag != 0 {
			exprs = append(exprs, attrSelector(s.column, name))
		}
	}
	return exprs
}

func (q *Querier) getMetricsLabelMapping(ctx context.Context, input []string) (r metricsLabelMapping, rerr error) {
	table := q.tables.Labels

	ctx, span := q.tracer.Start(ctx, "chstorage.metrics.getMetricsLabelMapping",
		trace.WithAttributes(
			attribute.StringSlice("chstorage.labels", input),
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
		name  = new(proto.ColStr).LowCardinality()
		scope = new(proto.ColEnum8)

		query = chsql.Select(table,
			chsql.Column("name", name),
			chsql.Column("scope", scope),
		).
			Where(chsql.In(
				chsql.Ident("name"),
				chsql.Ident("labels"),
			))
	)

	r.scope = make(map[string]labelScope, len(input))

	var inputData proto.ColStr
	for _, label := range input {
		inputData.Append(label)
	}
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < scope.Rows(); i++ {
				r.scope[name.Row(i)] |= labelScope(scope.Row(i))
			}
			return nil
		},
		ExternalTable: "labels",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},

		Type:   "getMetricsLabelMapping",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return r, err
	}
	span.AddEvent("mapping_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_labels", len(r.scope)),
	))

	return r, nil
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

type seriesKey struct {
	name       string
	attributes otelstorage.Hash
	scope      otelstorage.Hash
	resource   otelstorage.Hash
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

	mapping, err := p.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	var (
		points        []storage.Series
		expHistSeries []storage.Series
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.Points
		columns := newPointColumns()

		query, err := p.buildQuery(
			table, columns.ChsqlResult(),
			start, end,
			matchers,
			mapping,
		)
		if err != nil {
			return err
		}

		result, err := p.queryPoints(ctx, table, query, columns)
		if err != nil {
			return errors.Wrap(err, "query points")
		}
		points = result
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.ExpHistograms
		columns := newExpHistogramColumns()

		query, err := p.buildQuery(
			table, columns.ChsqlResult(),
			start, end,
			matchers,
			mapping,
		)
		if err != nil {
			return err
		}

		result, err := p.queryExpHistograms(ctx, table, query, columns)
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

func (p *promQuerier) buildQuery(
	table string, columns []chsql.ResultColumn,
	start, end time.Time,
	matchers []*labels.Matcher,
	mapping metricsLabelMapping,
) (*chsql.SelectQuery, error) {
	query := chsql.Select(table, columns...).
		Where(chsql.InTimeRange("timestamp", start, end))

	for _, m := range matchers {
		selectors := []chsql.Expr{
			chsql.Ident("name"),
		}
		if name := m.Name; name != labels.MetricName {
			selectors = mapping.Selectors(name)
		}

		expr, err := promQLLabelMatcher(selectors, m.Type, m.Value)
		if err != nil {
			return nil, err
		}
		query.Where(expr)
	}

	query.Order(chsql.Ident("timestamp"), chsql.Asc)
	return query, nil
}

func (p *promQuerier) queryPoints(ctx context.Context, table string, query *chsql.SelectQuery, c *pointColumns) ([]storage.Series, error) {
	type seriesWithLabels struct {
		series *series[pointData]
		labels map[string]string
	}

	set := map[seriesKey]seriesWithLabels{}
	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					name       = c.name.Row(i)
					value      = c.value.Row(i)
					timestamp  = c.timestamp.Row(i)
					attributes = c.attributes.Row(i)
					scope      = c.scope.Row(i)
					resource   = c.resource.Row(i)
				)
				key := seriesKey{
					name:       name,
					attributes: attributes.Hash(),
					scope:      scope.Hash(),
					resource:   resource.Hash(),
				}
				s, ok := set[key]
				if !ok {
					s = seriesWithLabels{
						series: &series[pointData]{},
						labels: map[string]string{},
					}
					set[key] = s
				}

				s.series.data.values = append(s.series.data.values, value)
				s.series.ts = append(s.series.ts, timestamp.UnixMilli())

				s.labels[labels.MetricName] = name
				attrsToLabels(attributes, s.labels)
				attrsToLabels(scope, s.labels)
				attrsToLabels(resource, s.labels)
			}
			return nil
		},

		Type:   "QueryPoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	var (
		result = make([]storage.Series, 0, len(set))
		lb     labels.ScratchBuilder
	)
	for _, s := range set {
		s.series.labels = buildPromLabels(&lb, s.labels)
		result = append(result, s.series)
	}

	return result, nil
}

func (p *promQuerier) queryExpHistograms(ctx context.Context, table string, query *chsql.SelectQuery, c *expHistogramColumns) ([]storage.Series, error) {
	type seriesWithLabels struct {
		series *series[expHistData]
		labels map[string]string
	}

	set := map[seriesKey]seriesWithLabels{}
	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					name                 = c.name.Row(i)
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
					attributes           = c.attributes.Row(i)
					scope                = c.scope.Row(i)
					resource             = c.resource.Row(i)
				)
				key := seriesKey{
					name:       name,
					attributes: attributes.Hash(),
					scope:      scope.Hash(),
					resource:   resource.Hash(),
				}
				s, ok := set[key]
				if !ok {
					s = seriesWithLabels{
						series: &series[expHistData]{},
						labels: map[string]string{},
					}
					set[key] = s
				}

				s.series.data.count = append(s.series.data.count, count)
				s.series.data.sum = append(s.series.data.sum, sum)
				s.series.data.min = append(s.series.data.min, vmin)
				s.series.data.max = append(s.series.data.max, vmax)
				s.series.data.scale = append(s.series.data.scale, scale)
				s.series.data.zerocount = append(s.series.data.zerocount, zerocount)
				s.series.data.positiveOffset = append(s.series.data.positiveOffset, positiveOffset)
				s.series.data.positiveBucketCounts = append(s.series.data.positiveBucketCounts, positiveBucketCounts)
				s.series.data.negativeOffset = append(s.series.data.negativeOffset, negativeOffset)
				s.series.data.negativeBucketCounts = append(s.series.data.negativeBucketCounts, negativeBucketCounts)
				s.series.ts = append(s.series.ts, timestamp.UnixMilli())

				s.labels[labels.MetricName] = name
				attrsToLabels(attributes, s.labels)
				attrsToLabels(scope, s.labels)
				attrsToLabels(resource, s.labels)
			}
			return nil
		},

		Type:   "QueryExpHistograms",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	var (
		result = make([]storage.Series, 0, len(set))
		lb     labels.ScratchBuilder
	)
	for _, s := range set {
		s.series.labels = buildPromLabels(&lb, s.labels)
		result = append(result, s.series)
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
