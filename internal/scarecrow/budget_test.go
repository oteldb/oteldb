package scarecrow_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// budgetCorpus has two series of ten samples each, so the sample counts the tests assert on are
// small enough to state exactly.
const budgetCorpus = `
load 10s
  counter{job="a"}   0 10 20 30 40 50 60 70 80 90
  counter{job="b"}   0 1 2 3 4 5 6 7 8 9
`

func TestMaxSamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxSamples int
		query      string
		wantErr    bool
	}{
		{
			name:       "disabled admits everything",
			maxSamples: 0,
			query:      `counter`,
			wantErr:    false,
		},
		{
			name:       "generous limit admits",
			maxSamples: 1_000_000,
			query:      `counter`,
			wantErr:    false,
		},
		{
			name:       "tight limit rejects",
			maxSamples: 1,
			query:      `counter`,
			wantErr:    true,
		},
		{
			name:       "range selector is charged",
			maxSamples: 1,
			query:      `rate(counter[1m])`,
			wantErr:    true,
		},
		{
			name:       "bare range selector is charged",
			maxSamples: 1,
			query:      `counter[1m]`,
			wantErr:    true,
		},
		{
			name:       "subquery is charged",
			maxSamples: 1,
			query:      `max_over_time(counter[1m:10s])`,
			wantErr:    true,
		},
		{
			name:       "aggregation over a rejected selector still rejects",
			maxSamples: 1,
			query:      `sum(counter)`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st := promqltest.LoadedStorage(t, budgetCorpus)
			t.Cleanup(func() { require.NoError(t, st.Close()) })

			e := scarecrow.NewEngine(scarecrow.Opts{MaxSamples: tt.maxSamples})

			q, err := e.NewInstantQuery(context.Background(), st, nil, tt.query, time.Unix(90, 0))
			require.NoError(t, err)

			defer q.Close()

			res := q.Exec(context.Background())

			if !tt.wantErr {
				require.NoError(t, res.Err)

				return
			}

			require.Error(t, res.Err)
			// The message must match upstream exactly: promhandler maps it to the same HTTP
			// status, and an operator switching engines should not see the wording change.
			assert.Equal(t,
				"query processing would load too many samples into memory in query execution",
				res.Err.Error(),
			)
			assert.ErrorAs(t, res.Err, new(promql.ErrTooManySamples))
		})
	}
}

// TestMaxSamplesSurvivesChunking is the regression guard that matters: the budget is per query,
// so splitting a range query into chunks must not hand each chunk a fresh allowance. With
// ChunkSteps=1 the query runs one chunk per step, which is exactly the shape that would evade a
// per-chunk budget.
func TestMaxSamplesSurvivesChunking(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, budgetCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	const maxSamples = 5

	for _, chunkSteps := range []int{-1, 1, 2, 1000} {
		t.Run(chunkStepsName(chunkSteps), func(t *testing.T) {
			t.Parallel()

			e := scarecrow.NewEngine(scarecrow.Opts{
				MaxSamples: maxSamples,
				ChunkSteps: chunkSteps,
			})

			q, err := e.NewRangeQuery(
				context.Background(), st, nil, `counter`,
				time.Unix(0, 0), time.Unix(90, 0), 10*time.Second,
			)
			require.NoError(t, err)

			defer q.Close()

			res := q.Exec(context.Background())
			require.Error(t, res.Err, "chunking must not reset the sample budget")
			assert.ErrorAs(t, res.Err, new(promql.ErrTooManySamples))
		})
	}
}

func chunkStepsName(n int) string {
	if n < 0 {
		return "unchunked"
	}

	return "chunk_steps_" + strconv.Itoa(n)
}

// TestMaxSamplesChargesPushdowns covers the production shape: with the native storage Scanner a
// pushed-down query reads no raw [scarecrow.Samples] at all, so if only the selector leaves
// charged the budget, exactly the queries the pushdown exists to accelerate — the big ones —
// would be the ones that escape it.
func TestMaxSamplesChargesPushdowns(t *testing.T) {
	t.Parallel()

	// Both the one-call grid path and the per-window fallback must charge, so run the same
	// query against a scanner that offers AggregateGrid and one that does not.
	for _, tt := range []struct {
		name string
		grid bool
	}{
		{"grid", true},
		{"per_window", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, qs := range []string{`count(counter)`, `max_over_time(counter[1m])`} {
				t.Run(qs, func(t *testing.T) {
					t.Parallel()

					store := promqltest.LoadedStorage(t, budgetCorpus)
					defer func() { require.NoError(t, store.Close()) }()

					e := scarecrow.NewEngine(scarecrow.Opts{
						MaxSamples: 1,
						NewScanner: func(q storage.Queryable) scarecrow.Scanner {
							s := &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
							if tt.grid {
								return s
							}

							return perWindowScanner{s: s}
						},
					})

					q, err := e.NewRangeQuery(
						context.Background(), store, nil, qs,
						time.Unix(0, 0), time.Unix(90, 0), 10*time.Second,
					)
					require.NoError(t, err)

					defer q.Close()

					res := q.Exec(context.Background())
					require.Error(t, res.Err, "a pushed-down query must not escape the budget")
					assert.ErrorAs(t, res.Err, new(promql.ErrTooManySamples))
				})
			}
		})
	}
}

func TestQueryCanceled(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, budgetCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	e := scarecrow.NewEngine(scarecrow.Opts{})

	q, err := e.NewInstantQuery(context.Background(), st, nil, `counter`, time.Unix(90, 0))
	require.NoError(t, err)

	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := q.Exec(ctx)
	require.Error(t, res.Err)
	assert.Equal(t, "query was canceled in query execution", res.Err.Error())
	assert.ErrorAs(t, res.Err, new(promql.ErrQueryCanceled))
}

// blockingScanner blocks in Series until the context ends, standing in for storage that is slow
// rather than absent. It makes "the deadline interrupts a query already in flight" testable
// without a sleep: the test finishes the instant the deadline fires.
type blockingScanner struct{}

func (blockingScanner) Series(
	ctx context.Context, _, _ int64, _ []*labels.Matcher,
) ([]labels.Labels, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (blockingScanner) Scan(
	ctx context.Context, _, _ int64, _ []*labels.Matcher,
) (scarecrow.SeriesIterator, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (blockingScanner) Close() error { return nil }

// TestQueryTimeout runs against a scanner that never returns, so the deadline is guaranteed to
// fire while the query is in flight. An already-elapsed deadline over real storage would be
// racy instead: whether the context is canceled before a ten-sample query finishes depends on
// the platform's timer granularity, which is coarse on Windows.
func TestQueryTimeout(t *testing.T) {
	t.Parallel()

	e := scarecrow.NewEngine(scarecrow.Opts{
		Timeout:    50 * time.Millisecond,
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return blockingScanner{} },
	})

	q, err := e.NewInstantQuery(context.Background(), nil, nil, `counter`, time.Unix(90, 0))
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(context.Background())
	require.Error(t, res.Err)
	// The message must match upstream exactly, for the same reason as in [TestMaxSamples].
	assert.Equal(t, "query timed out in query execution", res.Err.Error())
	assert.ErrorAs(t, res.Err, new(promql.ErrQueryTimeout))
}

// TestNoTimeoutByDefault pins the one place scarecrow deliberately differs from the upstream
// engine: there, a zero MaxSamples/Timeout fails every query, which makes a zero value a
// footgun. Here zero means "no limit", so an embedder that does not set them still works.
func TestNoTimeoutByDefault(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, budgetCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	e := scarecrow.NewEngine(scarecrow.Opts{})

	q, err := e.NewInstantQuery(context.Background(), st, nil, `sum(counter)`, time.Unix(90, 0))
	require.NoError(t, err)

	defer q.Close()

	require.NoError(t, q.Exec(context.Background()).Err)
}

// groupedCorpus has eight series in two `cpu` groups, so a `count by (cpu)` folds 8 series down
// to 2 values per step — the gap the charge has to respect.
const groupedCorpus = `
load 10s
  cpu_seconds{cpu="0",mode="user"}    0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="0",mode="system"}  0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="0",mode="idle"}    0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="0",mode="iowait"}  0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="1",mode="user"}    0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="1",mode="system"}  0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="1",mode="idle"}    0 1 2 3 4 5 6 7 8 9
  cpu_seconds{cpu="1",mode="iowait"}  0 1 2 3 4 5 6 7 8 9
`

// TestGridPushdownChargesResultNotIntermediate is the regression guard for the node-exporter
// dashboard: `count by (cpu)` was billed for the (series x step) grid it folded from rather than
// the (group x step) counts it keeps, so a panel over 256 series burned 737k of a 1M budget and
// the dashboard started failing with "too many samples". The pushdown exists to avoid that scan;
// charging for it defeated the purpose.
func TestGridPushdownChargesResultNotIntermediate(t *testing.T) {
	t.Parallel()

	// 8 series x 10 steps = 80 for the old charge; 2 groups x 10 steps = 20 for the new one.
	const maxSamples = 40

	for _, tt := range []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"count by keeps one value per group", `count(cpu_seconds) by (cpu)`, false},
		{"count keeps one value per step", `count(cpu_seconds)`, false},
		// A range aggregation's grid *is* its result, so it is still charged per (series, step)
		// and must still trip. Without this the fix would just be a hole in the budget.
		{"over_time is still charged per series", `max_over_time(cpu_seconds[1m])`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := promqltest.LoadedStorage(t, groupedCorpus)
			defer func() { require.NoError(t, store.Close()) }()

			e := scarecrow.NewEngine(scarecrow.Opts{
				MaxSamples: maxSamples,
				NewScanner: func(q storage.Queryable) scarecrow.Scanner {
					return &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
				},
			})

			q, err := e.NewRangeQuery(
				context.Background(), store, nil, tt.query,
				time.Unix(0, 0), time.Unix(90, 0), 10*time.Second,
			)
			require.NoError(t, err)

			defer q.Close()

			res := q.Exec(context.Background())

			if tt.wantErr {
				require.Error(t, res.Err)
				assert.ErrorAs(t, res.Err, new(promql.ErrTooManySamples))

				return
			}

			require.NoError(t, res.Err, "a folded count must not be billed for the grid it folded")
		})
	}
}
