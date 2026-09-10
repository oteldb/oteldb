package chstorage

import (
	"context"
	"maps"
	"slices"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logstorage"
)

// levelValues returns the distinct levels the matching records were written with, as
// [logqlabels.Level] spells them.
//
// The severity columns are the whole query: grouping by them reads two low-cardinality columns and
// nothing else, and the group count is bounded by the severity enum rather than by the window. So a
// tenant that only ever logs info and error offers two levels, instead of the fourteen the enum can
// name — which is what the old static list returned, whatever the data held.
func (q *Querier) levelValues(ctx context.Context, opts logstorage.LabelsOptions, limit int) ([]string, error) {
	table := q.tables.Logs

	queryLabels := make([]string, 0, len(opts.Query.Matchers))
	for _, m := range opts.Query.Matchers {
		queryLabels = append(queryLabels, string(m.Label))
	}
	mapping, err := q.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	var (
		severityNumber proto.ColUInt8
		severityText   = new(proto.ColStr).LowCardinality()

		query = chsql.Select(table,
			chsql.Column("severity_number", &severityNumber),
			chsql.Column("severity_text", severityText),
		).
			GroupBy(chsql.Ident("severity_number"), chsql.Ident("severity_text")).
			Where(chsql.InTimeRange("timestamp", opts.Start, opts.End, proto.PrecisionNano))
	)
	for _, m := range opts.Query.Matchers {
		query.Where(q.logQLLabelMatcher(m, mapping))
	}

	values := map[string]struct{}{}
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(_ context.Context, _ proto.Block) error {
			for i := 0; i < severityNumber.Rows(); i++ {
				var text string
				if i < severityText.Rows() {
					text = severityText.Row(i)
				}
				if v := logqlabels.Level(plog.SeverityNumber(severityNumber.Row(i)), text); v != "" {
					values[v] = struct{}{}
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

	out := slices.Sorted(maps.Keys(values))
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}
