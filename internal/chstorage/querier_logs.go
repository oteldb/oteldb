package chstorage

import (
	"context"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

var (
	_ logstorage.Querier  = (*Querier)(nil)
	_ logqlengine.Querier = (*Querier)(nil)
)

// LabelNames implements logstorage.Querier.
func (q *Querier) LabelNames(ctx context.Context, opts logstorage.LabelsOptions) (result []string, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LabelNames",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.String("chstorage.table", table),
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

	var (
		name  proto.ColStr
		dedup = map[string]struct{}{
			// Add materialized labels.
			logstorage.LabelTraceID:           {},
			logstorage.LabelSpanID:            {},
			logstorage.LabelSeverity:          {},
			logstorage.LabelBody:              {},
			logstorage.LabelServiceName:       {},
			logstorage.LabelServiceInstanceID: {},
			logstorage.LabelServiceNamespace:  {},
		}
	)
	if err := q.do(ctx, selectQuery{
		Query: chsql.SelectFrom(
			// Select deduplicated resources from subquery.
			q.deduplicatedResource(table, opts.Start, opts.End),
			chsql.ResultColumn{
				Name: "name",
				Expr: chsql.ArrayJoin(attrKeys(colResource)),
				Data: &name,
			}).
			Distinct(true).
			Limit(q.labelLimit),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < name.Rows(); i++ {
				// TODO: add configuration option
				label := otelstorage.KeyToLabel(name.Row(i))
				dedup[label] = struct{}{}
			}
			return nil
		},

		Type:   "LabelNames",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	// Deduplicate.
	result = maps.Keys(dedup)
	slices.Sort(result)

	return result, nil
}

type labelStaticIterator struct {
	name   string
	values []string
}

func (l *labelStaticIterator) Next(t *logstorage.Label) bool {
	if len(l.values) == 0 {
		return false
	}
	t.Name = l.name
	t.Value = l.values[0]
	l.values = l.values[1:]
	return true
}

func (l *labelStaticIterator) Err() error   { return nil }
func (l *labelStaticIterator) Close() error { return nil }

// LabelValues implements logstorage.Querier.
func (q *Querier) LabelValues(ctx context.Context, labelName string, opts logstorage.LabelsOptions) (riter iterators.Iterator[logstorage.Label], rerr error) {
	table := q.tables.Logs
	labelName = DecodeUnicodeLabel(labelName)

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.label", labelName),
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			attribute.Stringer("chstorage.matchers", opts.Query),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var values []string
	switch labelName {
	case logstorage.LabelBody, logstorage.LabelSpanID, logstorage.LabelTraceID:
	case logstorage.LabelSeverity:
		// FIXME(tdakkota): do a proper query with filtering
		values = []string{
			plog.SeverityNumberUnspecified.String(),
			plog.SeverityNumberTrace.String(),
			plog.SeverityNumberDebug.String(),
			plog.SeverityNumberInfo.String(),
			plog.SeverityNumberWarn.String(),
			plog.SeverityNumberError.String(),
			plog.SeverityNumberFatal.String(),
		}
		slices.Sort(values)
	default:
		queryLabels := make([]string, 1+len(opts.Query.Matchers))
		queryLabels = append(queryLabels, labelName)
		for _, m := range opts.Query.Matchers {
			queryLabels = append(queryLabels, string(m.Label))
		}

		mapping, err := q.getLabelMapping(ctx, queryLabels)
		if err != nil {
			return nil, errors.Wrap(err, "get label mapping")
		}
		if key, ok := mapping[labelName]; ok {
			labelName = key
		}

		resourceQuery := q.deduplicatedResource(table, opts.Start, opts.End)
		for _, m := range opts.Query.Matchers {
			resourceQuery.Where(q.logQLLabelMatcher(m, mapping))
		}
		var (
			value proto.ColStr
			query = chsql.SelectFrom(
				// Select deduplicated resources from subquery.
				resourceQuery,
				chsql.ResultColumn{
					Name: "value",
					Expr: attrSelector(colResource, labelName),
					Data: &value,
				}).
				Distinct(true).
				Order(chsql.Ident("value"), chsql.Asc).
				Limit(q.labelLimit)
		)
		if err := q.do(ctx, selectQuery{
			Query: query,
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < value.Rows(); i++ {
					if v := value.Row(i); v != "" {
						values = append(values, v)
					}
				}
				return nil
			},

			Type:   "LabelValues",
			Signal: "logs",
			Table:  table,
		}); err != nil {
			return nil, err
		}
	}
	span.AddEvent("values_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_values", len(values)),
	))

	return &labelStaticIterator{
		name:   labelName,
		values: values,
	}, nil
}

func (q *Querier) getLabelMapping(ctx context.Context, labels []string) (_ map[string]string, rerr error) {
	table := q.tables.LogAttrs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.getLabelMapping",
		trace.WithAttributes(
			attribute.StringSlice("chstorage.labels", labels),
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
		out   = make(map[string]string, len(labels))
		attrs = newLogAttrMapColumns()

		inputData proto.ColStr
	)
	for _, label := range labels {
		inputData.Append(label)
	}
	if err := q.do(ctx, selectQuery{
		Query: chsql.Select(q.tables.LogAttrs, attrs.ChsqlResult()...).
			Where(chsql.In(
				chsql.Ident("name"),
				chsql.Ident("labels"),
			)),
		OnResult: func(ctx context.Context, block proto.Block) error {
			attrs.ForEach(func(name, key string) {
				out[name] = key
			})
			return nil
		},
		ExternalTable: "labels",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},

		Type:   "getLabelMapping",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("mapping_fetched", trace.WithAttributes(
		xattribute.StringMap("chstorage.mapping", out),
	))

	return out, nil
}

func (q *Querier) getMaterializedLabelColumn(labelName string) (column chsql.Expr, isColumn bool) {
	switch labelName {
	case logstorage.LabelTraceID:
		return chsql.Hex(chsql.Ident("trace_id")), true
	case logstorage.LabelSpanID:
		return chsql.Hex(chsql.Ident("span_id")), true
	case logstorage.LabelSeverity:
		return chsql.Ident("severity_text"), true
	case logstorage.LabelBody:
		return chsql.Ident("body"), true
	case logstorage.LabelServiceName, logstorage.LabelServiceNamespace, logstorage.LabelServiceInstanceID:
		return chsql.Ident(labelName), true
	default:
		return column, false
	}
}

// Series returns all available log series.
func (q *Querier) Series(ctx context.Context, opts logstorage.SeriesOptions) (result logstorage.Series, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.Series",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", opts.Start),
			xattribute.UnixNano("chstorage.range.end", opts.End),
			xattribute.StringerSlice("chstorage.selectors", opts.Selectors),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			span.AddEvent("series_fetched", trace.WithAttributes(
				attribute.Int("chstorage.total_series", len(result)),
			))
		}
		span.End()
	}()

	var materializedMap chsql.Expr
	{
		var (
			materialized = []string{
				logstorage.LabelSeverity,
				logstorage.LabelServiceName,
				logstorage.LabelServiceInstanceID,
				logstorage.LabelServiceNamespace,
			}
			entries = make([]chsql.Expr, 0, len(materialized)*2)
		)
		for _, label := range materialized {
			expr, _ := q.getMaterializedLabelColumn(label)
			entries = append(entries, chsql.String(label), expr)
		}

		materializedMap = chsql.Map(entries...)
	}

	var (
		series = proto.NewMap(
			new(proto.ColStr),
			new(proto.ColStr),
		)

		query = chsql.Select(table, chsql.ResultColumn{
			Name: "series",
			Expr: chsql.MapConcat(
				materializedMap,
				attrStringMap(colResource),
			),
			Data: series,
		}).
			Distinct(true).
			Where(chsql.InTimeRange("timestamp", opts.Start, opts.End, proto.PrecisionNano))
	)
	if sels := opts.Selectors; len(sels) > 0 {
		// Gather all labels for mapping fetch.
		labels := make([]string, 0, len(sels))
		for _, sel := range sels {
			for _, m := range sel.Matchers {
				labels = append(labels, string(m.Label))
			}
		}
		mapping, err := q.getLabelMapping(ctx, labels)
		if err != nil {
			return nil, errors.Wrap(err, "get label mapping")
		}

		sets := make([]chsql.Expr, 0, len(sels))
		for _, sel := range sels {
			selExprs := make([]chsql.Expr, 0, len(sel.Matchers))
			for _, m := range sel.Matchers {
				selExprs = append(selExprs, q.logQLLabelMatcher(m, mapping))
			}
			sets = append(sets, chsql.JoinAnd(selExprs...))
		}
		if len(sets) > 0 {
			query.Where(chsql.JoinOr(sets...))
		}
	}

	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < series.Rows(); i++ {
				s := make(map[string]string)
				forEachColMap(series, i, func(k, v string) {
					if k == "" {
						return
					}
					s[otelstorage.KeyToLabel(k)] = v
				})
				result = append(result, s)
			}
			return nil
		},

		Type:   "Series",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (q *Querier) deduplicatedResource(table string, start, end time.Time) *chsql.SelectQuery {
	// Select deduplicated resource by using GROUP BY, since DISTINCT is not optimized by Clickhouse.
	//
	// See https://github.com/ClickHouse/ClickHouse/issues/4670
	return chsql.Select(table, chsql.Column(colResource, nil)).
		GroupBy(chsql.Ident(colResource)).
		Where(chsql.InTimeRange("timestamp", start, end, proto.PrecisionNano))
}

func forEachColMap[K comparable, V any](c *proto.ColMap[K, V], row int, cb func(K, V)) {
	var start int
	end := int(c.Offsets[row])
	if row > 0 {
		start = int(c.Offsets[row-1])
	}
	for idx := start; idx < end; idx++ {
		cb(c.Keys.Row(idx), c.Values.Row(idx))
	}
}
