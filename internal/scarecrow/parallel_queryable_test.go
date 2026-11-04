package scarecrow_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type parallelFallbackQueryable struct {
	storage.Queryable
	selects atomic.Int64
}

func (q *parallelFallbackQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return parallelFallbackQuerier{Querier: querier, owner: q}, nil
}

type parallelFallbackQuerier struct {
	storage.Querier
	owner *parallelFallbackQueryable
}

func (q parallelFallbackQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.owner.selects.Add(1)
	return q.Querier.Select(ctx, sorted, hints, matchers...)
}

func TestParallelFoldQueryableDoesNotAmplifyReads(t *testing.T) {
	t.Parallel()
	store := promqltest.LoadedStorage(t, benchCorpus(8))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	queryable := &parallelFallbackQueryable{Queryable: store}
	engine := scarecrow.NewEngine(scarecrow.Opts{})
	query, err := engine.NewRangeQuery(t.Context(), queryable, nil, `avg(avg_over_time(gauge[1m]))`,
		time.Unix(60, 0), time.Unix(600, 0), 15*time.Second)
	require.NoError(t, err)
	result := query.Exec(t.Context())
	query.Close()
	require.NoError(t, result.Err)
	require.EqualValues(t, 2, queryable.selects.Load(), "one schema enumeration and one sample scan, regardless of worker slots")
}
