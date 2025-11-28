package chstorage

import (
	"context"
	"slices"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/xattribute"
)

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
	table := p.tables.Timeseries

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

	var (
		value      proto.ColumnOf[string]
		columnExpr chsql.Expr
	)
	if labelName == labels.MetricName {
		columnExpr = chsql.Ident("name")
		value = proto.NewLowCardinality(&proto.ColStr{})
	} else {
		columnExpr = firstAttrSelector(labelName)
		value = &proto.ColStr{}
	}

	query := chsql.Select(table, chsql.ResultColumn{
		Name: "value",
		Expr: columnExpr,
		Data: value,
	}).
		Distinct(true)

	for _, m := range matchers {
		selectors := []chsql.Expr{
			chsql.Ident("name"),
		}
		if name := m.Name; name != labels.MetricName {
			selectors = []chsql.Expr{
				attrSelector(colAttrs, name),
				attrSelector(colScope, name),
				attrSelector(colResource, name),
			}
		}

		expr, err := promQLLabelMatcher(selectors, m.Type, m.Value)
		if err != nil {
			return nil, err
		}
		query.Where(expr)
	}
	query.GroupBy(
		chsql.Ident("name"),
		chsql.Ident("attribute"),
		chsql.Ident("scope"),
		chsql.Ident("resource"),
	)
	timeseriesInRange(query, p.mint, p.maxt, newTimeseriesColumns().timestampPrecision())
	query.Limit(p.labelLimit)

	var result []string
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

	slices.Sort(result)
	// Remove duplicates.
	result = slices.Clip(result)

	return result, nil
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
	table := p.tables.Timeseries

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
			Distinct(true)
	)
	for _, m := range matchers {
		selectors := []chsql.Expr{
			chsql.Ident("name"),
		}
		if name := m.Name; name != labels.MetricName {
			selectors = []chsql.Expr{
				attrSelector(colAttrs, name),
				attrSelector(colScope, name),
				attrSelector(colResource, name),
			}
		}

		expr, err := promQLLabelMatcher(selectors, m.Type, m.Value)
		if err != nil {
			return nil, err
		}
		query.Where(expr)
	}
	query.GroupBy(
		chsql.Ident("name"),
		chsql.Ident("attribute"),
		chsql.Ident("scope"),
		chsql.Ident("resource"),
	)
	query.Limit(p.labelLimit)

	var result []string
	if err := p.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < value.Rows(); i++ {
				result = append(result, value.Row(i))
			}
			return nil
		},

		Type:   "getMatchingLabelNames",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("labels_fetched", trace.WithAttributes(
		attribute.String("chstorage.table", table),
		attribute.Int("chstorage.total_values", len(result)),
	))

	if len(result) > 0 {
		// Add `__name__` only if there is any matching series.
		result = append(result, labels.MetricName)
	}

	slices.Sort(result)
	// Remove duplicates.
	result = slices.Clip(result)

	return result, nil
}
