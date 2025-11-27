package chstorage

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	singleflight "github.com/go-faster/sdk/singleflightx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type timeseriesQuerier struct {
	tables Tables
	do     func(ctx context.Context, s selectQuery) error

	sg singleflight.Group[xxh3.Uint128, metricsTimeseries]

	tracer trace.Tracer
}

func newTimeseriesQuerier(q *Querier) *timeseriesQuerier {
	return &timeseriesQuerier{
		tables: q.tables,
		do:     q.do,
		tracer: q.tracer,
	}
}

type (
	metricsTimeseries          = map[[16]byte]labels.Labels
	queryMetricsTimeseriesFunc = func(ctx context.Context, matcherSets [][]*labels.Matcher) (metricsTimeseries, error)
)

func (q *timeseriesQuerier) hashMatchers(sets [][]*labels.Matcher) xxh3.Uint128 {
	size := 0
	for _, set := range sets {
		size += len(set)
	}

	type pair struct {
		Type  labels.MatchType
		Name  string
		Value string
	}
	var pairs []pair
	const mapStackThreshold = 16
	if l := size; l < mapStackThreshold {
		pairs = make([]pair, 0, mapStackThreshold)
	} else {
		pairs = make([]pair, 0, size)
	}
	for _, set := range sets {
		for _, m := range set {
			pairs = append(pairs, pair{
				Type:  m.Type,
				Name:  m.Name,
				Value: m.Value,
			})
		}
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		if r := cmp.Compare(a.Type, b.Type); r != 0 {
			return r
		}
		return cmp.Or(
			strings.Compare(a.Name, b.Name),
			strings.Compare(a.Value, b.Value),
		)
	})

	h := xxh3.New()
	for _, p := range pairs {
		_, _ = h.Write([]byte{byte(p.Type)})
		_, _ = h.WriteString(p.Name)
		_, _ = h.WriteString(p.Value)
	}
	return h.Sum128()
}

func (q *timeseriesQuerier) Query(ctx context.Context, matcherSets [][]*labels.Matcher) (_ map[[16]byte]labels.Labels, rerr error) {
	ctx, span := q.tracer.Start(ctx, "chstorage.metrics.timeseries.Query")
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	matchersHash := q.hashMatchers(matcherSets)
	resultCh := q.sg.DoChan(matchersHash, func() (metricsTimeseries, error) {
		link := trace.LinkFromContext(ctx)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		return q.queryTimeseries(ctx, link, matcherSets)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resultCh:
		result, shared, err := r.Val, r.Shared, r.Err
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Query did not complete, probably stuck.
			span.AddEvent("retry_query")
			// Try again without singleflight.
			result, err = q.queryTimeseries(ctx, trace.Link{}, matcherSets)
			shared = false
		}
		if err != nil {
			return nil, err
		}

		span.AddEvent("timeseries_fetched", trace.WithAttributes(
			attribute.Int("chstorage.total_series", len(r.Val)),
			attribute.Bool("chstorage.shared_result", shared),
		))
		return result, nil
	}
}

func (q *timeseriesQuerier) queryTimeseries(ctx context.Context, link trace.Link, matcherSets [][]*labels.Matcher) (_ map[[16]byte]labels.Labels, rerr error) {
	table := q.tables.Timeseries

	ctx, span := q.tracer.Start(ctx, "chstorage.metrics.timeseries.queryTimeseries",
		trace.WithAttributes(
			attribute.String("chstorage.table", table),
		),
		trace.WithLinks(link),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var (
		c           = newTimeseriesColumns()
		selectExprs = MergeColumns(
			Columns{
				{Name: "name", Data: c.name},
			},
			c.attributes.Columns(),
			c.scope.Columns(),
			c.resource.Columns(),
		).ChsqlResult()
	)
	selectExprs = append(selectExprs, chsql.ResultColumn{
		Name: "hash",
		Expr: chsql.Function("any", chsql.Ident("hash")),
		Data: c.hash,
	})

	var (
		query = chsql.Select(table, selectExprs...)
		sets  = make([]chsql.Expr, 0, len(matcherSets))
	)
	for _, set := range matcherSets {
		matchers := make([]chsql.Expr, 0, len(set))
		for _, m := range set {
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

			matcher, err := promQLLabelMatcher(selectors, m.Type, m.Value)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, matcher)
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

	var (
		set = map[[16]byte]labels.Labels{}
		lb  labels.ScratchBuilder
	)
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.name.Rows(); i++ {
				var (
					name       = c.name.Row(i)
					hash       = c.hash.Row(i)
					attributes = c.attributes.Row(i)
					scope      = c.scope.Row(i)
					resource   = c.resource.Row(i)
				)

				_, ok := set[hash]
				if !ok {
					lb.Reset()
					for k, v := range attributes.AsMap().All() {
						lb.Add(k, v.AsString())
					}
					for k, v := range scope.AsMap().All() {
						lb.Add(k, v.AsString())
					}
					for k, v := range resource.AsMap().All() {
						lb.Add(k, v.AsString())
					}
					lb.Add("__name__", name)
					lb.Sort()
					set[hash] = lb.Labels()
				}
			}
			return nil
		},

		Type:   "QueryTimeseries",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("timeseries_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
	))

	return set, nil
}
