package storagebackend

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
)

func TestPartitionIteratorIndependentBorrows(t *testing.T) {
	t.Parallel()
	source := &countingIterator{batches: []*fetch.Batch{testBatch("a", 3), testBatch("b", 3), testBatch("c", 3)}}
	released := 0
	for _, batch := range source.batches {
		batch.SetRelease(func(b *fetch.Batch) {
			released++
			clear(b.Values)
		})
	}
	shared := &partitionScan{iter: source, remaining: 2}
	left, right := &partitionIterator{shared: shared}, &partitionIterator{shared: shared}
	a, err := left.Next(context.Background())
	require.NoError(t, err)
	b, err := right.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, []float64{0, 1, 2}, a.V, "advancing another partition must preserve this borrow")
	require.Equal(t, []float64{0, 1, 2}, b.V)
	require.Equal(t, 0, left.Position())
	require.Equal(t, 1, right.Position())
	require.Zero(t, released)
	left.Release()
	require.Equal(t, 1, released)
	require.Equal(t, []float64{0, 1, 2}, b.V)
	require.NoError(t, left.Close())
	require.False(t, source.closed)
	c, err := right.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "c", c.Labels.Get("__name__"))
	require.Equal(t, 2, right.Position())
	require.Equal(t, 2, released)
	require.NoError(t, right.Close())
	require.True(t, source.closed)
	require.Equal(t, 3, released)
	require.NoError(t, right.Close())
	require.Equal(t, 3, released)
}

type recoveringPartitionSource struct {
	calls int
	err   error
}

func (s *recoveringPartitionSource) Next(context.Context) (*fetch.Batch, error) {
	s.calls++
	if s.calls == 1 {
		return nil, s.err
	}
	return testBatch("unexpected", 1), nil
}

func (*recoveringPartitionSource) Close() error { return nil }

func TestPartitionIteratorRetainsFirstFailure(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("fetch failed")
	source := &recoveringPartitionSource{err: sentinel}
	shared := &partitionScan{iter: source, remaining: 2}
	left, right := &partitionIterator{shared: shared}, &partitionIterator{shared: shared}
	_, err := left.Next(t.Context())
	require.ErrorIs(t, err, sentinel)
	_, err = right.Next(t.Context())
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, source.calls, "another partition must not resume a failed source")
	require.NoError(t, left.Close())
	require.NoError(t, right.Close())
}

func TestPartitionIteratorMemoryBudget(t *testing.T) {
	t.Parallel()
	source := &countingIterator{batches: []*fetch.Batch{testBatch("a", 3)}}
	budget := readbudget.New(1000)
	iter := &partitionIterator{shared: &partitionScan{iter: source, remaining: 1}, budget: budget}
	_, err := iter.Next(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1000-3*24, budget.Remaining())
	iter.Release()
	require.Nil(t, iter.batch.cur.T)
	require.Nil(t, iter.batch.cur.V)
	require.Nil(t, iter.batch.cur.Weights)
	require.EqualValues(t, 1000-3*8, budget.Remaining(), "only conversion scratch survives Release")
	require.NoError(t, iter.Close())
	require.Nil(t, iter.batch.ts)
	require.EqualValues(t, 1000, budget.Remaining())
}
