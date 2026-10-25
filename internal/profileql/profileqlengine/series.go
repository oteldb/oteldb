package profileqlengine

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/xspan"
)

// SelectSeries returns the total profile value in each half-open time bucket.
func (e *Engine) SelectSeries(ctx context.Context, query string, start, end time.Time, step time.Duration) (values []int64, rerr error) {
	ctx, span := e.tracer.Start(ctx, "SelectSeries", trace.WithAttributes(attribute.String("profileql.query", query)))
	defer func() { xspan.End(span, rerr) }()
	expr, err := profileql.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}
	params := profilestorage.SelectSeriesParams{Type: expr.Type, Matchers: expr.Matchers, Start: start, End: end, Step: step}
	n, err := params.NumBuckets()
	if err != nil {
		return nil, err
	}
	if q, ok := e.querier.(profilestorage.SeriesQuerier); ok {
		values, err := q.SelectSeries(ctx, params)
		if err != nil {
			return nil, errors.Wrap(err, "select profile series")
		}
		if len(values) != n {
			return nil, errors.Errorf("profile series returned %d buckets, want %d", len(values), n)
		}
		return values, nil
	}
	values = make([]int64, n)
	for i := range values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		from := start.Add(time.Duration(i) * step)
		to := from.Add(step)
		if to.After(end) {
			to = end
		}
		tree, err := e.querier.SelectMergeProfile(ctx, profilestorage.SelectProfileParams{Type: expr.Type, Matchers: expr.Matchers, Start: from, End: to.Add(-time.Nanosecond)})
		if err != nil {
			return nil, errors.Wrap(err, "select merge profile")
		}
		values[i] = tree.Total()
	}
	return values, nil
}
