package chstorage

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
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
		queryTimeseries: q.queryMetricsTimeseries,
		do:              q.do,

		tracer: q.tracer,
	}, nil
}

type exemplarQuerier struct {
	ctx context.Context

	ch              ClickHouseClient
	tables          Tables
	queryTimeseries func(ctx context.Context, start, end time.Time, matcherSets [][]*labels.Matcher) (map[[16]byte]labels.Labels, error)
	do              func(ctx context.Context, s selectQuery) error

	tracer trace.Tracer
}

var _ storage.ExemplarQuerier = (*exemplarQuerier)(nil)

func (q *exemplarQuerier) Select(startMs, endMs int64, matcherSets ...[]*labels.Matcher) (_ []exemplar.QueryResult, rerr error) {
	table := q.tables.Exemplars
	start, end := q.extractParams(startMs, endMs)

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

	timeseries, err := q.queryTimeseries(ctx, start, end, matcherSets)
	if err != nil {
		return nil, errors.Wrap(err, "query timeseries hashes")
	}

	var (
		c     = newExemplarColumns()
		query = chsql.Select(table, c.ChsqlResult()...).
			Where(
				chsql.InTimeRange("timestamp", start, end, c.exemplarTimestamp.Precision),
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
		set            = map[[16]byte]exemplar.QueryResult{}
		totalExemplars int

		lb labels.ScratchBuilder
	)
	if err := q.do(ctx, selectQuery{
		Query:         query,
		ExternalTable: "timeseries_hashes",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				var (
					hash               = c.hash.Row(i)
					filteredAttributes = c.filteredAttributes.Row(i)
					exemplarTimestamp  = c.exemplarTimestamp.Row(i)
					value              = c.value.Row(i)
					spanID             = c.spanID.Row(i)
					traceID            = c.traceID.Row(i)
				)
				s, ok := set[hash]
				if !ok {
					lb, ok := timeseries[hash]
					if !ok {
						zctx.From(ctx).Error("Can't find labels for requested series")
						continue
					}
					s = exemplar.QueryResult{
						SeriesLabels: lb,
					}
				}

				exemplarLabels := map[string]string{
					"span_id":  hex.EncodeToString(spanID[:]),
					"trace_id": hex.EncodeToString(traceID[:]),
				}
				if err := parseLabels(filteredAttributes, exemplarLabels); err != nil {
					return errors.Wrap(err, "parse filtered attributes")
				}
				s.Exemplars = append(s.Exemplars, exemplar.Exemplar{
					Labels: buildPromLabels(&lb, exemplarLabels),
					Value:  value,
					Ts:     exemplarTimestamp.UnixMilli(),
					HasTs:  true,
				})
				set[hash] = s

				totalExemplars++
			}
			return nil
		},

		Type:   "QueryExemplars",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("exemplars_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_exemplars", totalExemplars),
	))

	result := make([]exemplar.QueryResult, 0, len(set))
	for _, qr := range set {
		result = append(result, qr)
	}
	return result, nil
}

func (q *exemplarQuerier) extractParams(startMs, endMs int64) (start, end time.Time) {
	if startMs != promapi.MinTime.UnixMilli() {
		start = time.UnixMilli(startMs)
	}
	if endMs != promapi.MaxTime.UnixMilli() {
		end = time.UnixMilli(endMs)
	}
	return start, end
}
