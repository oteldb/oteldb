package chstorage

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

var _ storage.ExemplarQueryable = (*Querier)(nil)

// Querier returns a new Querier on the storage.
func (q *Querier) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &exemplarQuerier{
		ctx: ctx,

		ch:              q.ch,
		tables:          q.tables,
		getLabelMapping: q.getMetricsLabelMapping,
		do:              q.do,

		tracer: q.tracer,
	}, nil
}

type exemplarQuerier struct {
	ctx context.Context

	ch              ClickHouseClient
	tables          Tables
	getLabelMapping func(context.Context, []string) (metricsLabelMapping, error)
	do              func(ctx context.Context, s selectQuery) error

	tracer trace.Tracer
}

var _ storage.ExemplarQuerier = (*exemplarQuerier)(nil)

func (q *exemplarQuerier) Select(startMs, endMs int64, matcherSets ...[]*labels.Matcher) (_ []exemplar.QueryResult, rerr error) {
	table := q.tables.Exemplars
	start, end, queryLabels := q.extractParams(startMs, endMs, matcherSets)

	ctx, span := q.tracer.Start(q.ctx, "chstorage.exemplars.Select",
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

	mapping, err := q.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	c := newExemplarColumns()
	query, err := q.buildQuery(
		table, c.ChsqlResult(),
		start, end,
		matcherSets,
		mapping,
	)
	if err != nil {
		return nil, err
	}

	type groupedExemplars struct {
		labels    map[string]string
		exemplars []exemplar.Exemplar
	}
	var (
		set = map[seriesKey]*groupedExemplars{}
		lb  labels.ScratchBuilder
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					name               = c.name.Row(i)
					filteredAttributes = c.filteredAttributes.Row(i)
					exemplarTimestamp  = c.exemplarTimestamp.Row(i)
					value              = c.value.Row(i)
					spanID             = c.spanID.Row(i)
					traceID            = c.traceID.Row(i)
					attributes         = c.attributes.Row(i)
					scope              = c.scope.Row(i)
					resource           = c.resource.Row(i)
				)
				key := seriesKey{
					name:       name,
					attributes: attributes.Hash(),
					scope:      scope.Hash(),
					resource:   resource.Hash(),
				}
				s, ok := set[key]
				if !ok {
					s = &groupedExemplars{
						labels: map[string]string{},
					}
					set[key] = s
				}

				exemplarLabels := map[string]string{
					"span_id":  hex.EncodeToString(spanID[:]),
					"trace_id": hex.EncodeToString(traceID[:]),
				}
				if err := parseLabels(filteredAttributes, exemplarLabels); err != nil {
					return errors.Wrap(err, "parse filtered attributes")
				}
				s.exemplars = append(s.exemplars, exemplar.Exemplar{
					Labels: buildPromLabels(&lb, exemplarLabels),
					Value:  value,
					Ts:     exemplarTimestamp.UnixMilli(),
					HasTs:  true,
				})

				s.labels[labels.MetricName] = name
				attrsToLabels(attributes, s.labels)
				attrsToLabels(scope, s.labels)
				attrsToLabels(resource, s.labels)
			}
			return nil
		},

		Type:   "QueryExemplars",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	result := make([]exemplar.QueryResult, 0, len(set))
	for _, group := range set {
		result = append(result, exemplar.QueryResult{
			SeriesLabels: buildPromLabels(&lb, group.labels),
			Exemplars:    group.exemplars,
		})
	}
	return result, nil
}

func (q *exemplarQuerier) extractParams(startMs, endMs int64, matcherSets [][]*labels.Matcher) (start, end time.Time, mlabels []string) {
	if startMs != promapi.MinTime.UnixMilli() {
		start = time.UnixMilli(startMs)
	}
	if endMs != promapi.MaxTime.UnixMilli() {
		end = time.UnixMilli(endMs)
	}
	for _, set := range matcherSets {
		for _, m := range set {
			mlabels = append(mlabels, m.Name)
		}
	}
	return start, end, mlabels
}

func (q *exemplarQuerier) buildQuery(
	table string, columns []chsql.ResultColumn,
	start, end time.Time,
	matcherSets [][]*labels.Matcher,
	mapping metricsLabelMapping,
) (*chsql.SelectQuery, error) {
	query := chsql.Select(table, columns...).
		Where(chsql.InTimeRange("timestamp", start, end))

	sets := make([]chsql.Expr, 0, len(matcherSets))
	for _, set := range matcherSets {
		matchers := make([]chsql.Expr, 0, len(set))
		for _, m := range set {
			selectors := []chsql.Expr{
				chsql.Ident("name"),
			}
			if name := m.Name; name != labels.MetricName {
				selectors = mapping.Selectors(name)
			}

			matcher, err := promQLLabelMatcher(selectors, m.Type, m.Value)
			if err != nil {
				return query, err
			}
			matchers = append(matchers, matcher)
		}
		sets = append(sets, chsql.JoinAnd(matchers...))
	}

	return query.
		Where(chsql.JoinOr(sets...)).
		Order(chsql.Ident("timestamp"), chsql.Asc), nil
}
