package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// budgetSpy counts the per-query budget installs a [storagebackend.Source] is asked for, and keeps
// the allowance each one produced. It delegates everything else to the engine underneath.
type budgetSpy struct {
	storagebackend.Source

	calls    int
	budgets  []*readbudget.Budget
	installs []context.Context
}

func (s *budgetSpy) WithQueryBudget(ctx context.Context) context.Context {
	s.calls++
	out := s.Source.WithQueryBudget(ctx)
	s.budgets = append(s.budgets, readbudget.From(out))
	s.installs = append(s.installs, ctx)

	return out
}

// openSpied builds a read-only backend whose Source records every per-query budget install.
func openSpied(
	t *testing.T, maxQueryBytes int64,
) (spy *budgetSpy, b *storagebackend.Backend, start, end time.Time) {
	t.Helper()

	ctx := context.Background()
	store, err := storage.Open(ctx, storage.Options{MaxQueryBytes: maxQueryBytes},
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	start = time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	const span = 4 * time.Minute
	require.NoError(t, storagebackend.New(store).ConsumeLogs(ctx, genSpreadLogs(4000, start, span)))

	spy = &budgetSpy{Source: store}

	return spy, storagebackend.NewQuery(spy), start, start.Add(span)
}

func evalLogQL(t *testing.T, b *storagebackend.Backend, query string, start, end time.Time) error {
	t.Helper()

	ctx := context.Background()
	engine, err := logqlengine.NewEngine(b.Logs(), logqlengine.Options{})
	require.NoError(t, err)

	q, err := engine.NewQuery(ctx, query)
	require.NoError(t, err)

	_, err = q.Eval(ctx, logqlengine.EvalParams{
		Start: start, End: end, Step: 30 * time.Second,
		Direction: logqlengine.DirectionForward, Limit: -1,
	})

	return err
}

// The engine call must open the budget itself. Without this the storage library still bounds each
// individual fetch, so the query is not unbounded — but the several fetches one query makes each
// take a fresh allowance, and nothing bounds the query as a whole.
func TestQueryBudgetInstalledAtEngineBoundary(t *testing.T) {
	t.Parallel()

	spy, b, start, end := openSpied(t, 1<<30)

	require.NoError(t, evalLogQL(t, b, `{service_name="api"}`, start, end))

	require.NotZero(t, spy.calls, "the engine call did not open a query budget")
	for i, ctx := range spy.installs {
		assert.Nil(t, readbudget.From(ctx), "install %d was handed a ctx that already had a budget", i)
	}
	for i, b := range spy.budgets {
		assert.NotNil(t, b, "install %d produced no allowance", i)
	}
}

// The allowance must come back when the query ends. A reservation held past the query would shrink
// every later query until the process answered nothing.
func TestQueryBudgetReleasedBetweenQueries(t *testing.T) {
	t.Parallel()

	spy, b, start, end := openSpied(t, 1<<30)

	for range 3 {
		require.NoError(t, evalLogQL(t, b, `{service_name="api"}`, start, end))
	}

	require.NotEmpty(t, spy.budgets)
	for i, budget := range spy.budgets {
		require.NotNil(t, budget)
		assert.Equal(t, int64(1<<30), budget.Remaining(),
			"query %d ended still holding %d bytes", i, int64(1<<30)-budget.Remaining())
	}
}

// A configured bound must actually reach the record read path, not merely be stored.
func TestQueryBudgetRefusesOversizedRead(t *testing.T) {
	t.Parallel()

	_, b, start, end := openSpied(t, 1)

	err := evalLogQL(t, b, `{service_name="api"}`, start, end)
	require.Error(t, err)
	assert.ErrorIs(t, err, readbudget.ErrExceeded)
}
