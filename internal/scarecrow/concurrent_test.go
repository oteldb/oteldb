package scarecrow

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestMain asserts that no goroutine outlives the test binary. It is checked here rather than
// per-test because goleak run inside a t.Parallel() test sees its siblings as leaks; verifying
// once after every test has finished is both correct and stricter.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// countingOp emits n single-step columns, then optionally an error. It records how far it was
// driven, so a test can tell prefetching from full consumption.
type countingOp struct {
	n      int
	served int
	fail   error
	closed bool

	out Column
}

func (o *countingOp) String() string       { return "counting" }
func (o *countingOp) Children() []Operator { return nil }

func (o *countingOp) Close() error {
	o.closed = true

	return nil
}

func (o *countingOp) Schema(context.Context) (*Schema, error) {
	series := make([]labels.Labels, o.n)
	for i := range series {
		series[i] = labels.FromStrings("i", string(rune('a'+i)))
	}

	return NewSchema(series), nil
}

func (o *countingOp) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.served >= o.n {
		if o.fail != nil {
			return nil, o.fail
		}

		return nil, nil
	}

	o.out.Resize(SeriesRef(o.served), 1)
	o.out.Set(0, float64(o.served))
	o.served++

	return &o.out, nil
}

// drain reads an operator to completion, returning the values it produced.
func drain(t *testing.T, op Operator) []float64 {
	t.Helper()

	var got []float64

	for {
		col, err := op.Next(context.Background())
		require.NoError(t, err)

		if col == nil {
			return got
		}

		got = append(got, col.V[0])
	}
}

// TestConcurrentPassesEveryColumnInOrder is what guards the ring. With a single output buffer
// the producer would overwrite a column that is still queued in the channel, and the consumer
// would read duplicated or skipped values; driving more columns than the prefetch depth makes
// that certain to happen.
//
// Note what is *not* asserted: that a column survives a later Next. The contract says a column
// is valid only until the next Next or Close, and the ring exists to protect columns in flight,
// not to let a consumer retain one.
func TestConcurrentPassesEveryColumnInOrder(t *testing.T) {
	t.Parallel()

	child := &countingOp{n: 20}
	op := newConcurrent(child)

	defer func() { require.NoError(t, op.Close()) }()

	got := drain(t, op)
	require.Len(t, got, 20)

	for i, v := range got {
		require.InDeltaf(t, float64(i), v, 0, "column %d out of order", i)
	}
}

func TestConcurrentPropagatesError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	op := newConcurrent(&countingOp{n: 3, fail: sentinel})

	defer func() { require.NoError(t, op.Close()) }()

	ctx := context.Background()

	for range 3 {
		col, err := op.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, col)
	}

	_, err := op.Next(ctx)
	require.ErrorIs(t, err, sentinel)
}

// TestConcurrentCancellationStopsProducer covers the case the ring and the drain in Close exist
// for: abandoning a query part-way must not leave a producer blocked on a full channel.
func TestConcurrentCancellationStopsProducer(t *testing.T) {
	t.Parallel()

	// Far more columns than the prefetch depth, so the producer is certainly blocked on send.
	op := newConcurrent(&countingOp{n: 10_000})

	ctx, cancel := context.WithCancel(context.Background())

	col, err := op.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, col)

	cancel()

	// Close must return rather than hang, and the producer goroutine must exit.
	require.NoError(t, op.Close())
}

// TestConcurrentCloseWithoutDrain is the common shape when a query errors early: the consumer
// stops reading and closes while the producer is mid-stream.
func TestConcurrentCloseWithoutDrain(t *testing.T) {
	t.Parallel()

	op := newConcurrent(&countingOp{n: 10_000})

	_, err := op.Next(context.Background())
	require.NoError(t, err)

	require.NoError(t, op.Close())
}

func TestConcurrentCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	op := newConcurrent(&countingOp{n: 5})

	require.NoError(t, op.Close())
	require.NoError(t, op.Close())
}

// TestConcurrentFallsBackInline pins the degradation path: with no slot free the operator runs
// its child synchronously rather than waiting, so contention costs parallelism, never liveness.
func TestConcurrentFallsBackInline(t *testing.T) {
	t.Parallel()

	// An exhausted private semaphore, so the process-wide one that parallel tests share is
	// left alone.
	full := newSemaphore(1)
	require.True(t, full.tryAcquire())

	op := newConcurrent(&countingOp{n: 5})
	op.slots = full

	defer func() { require.NoError(t, op.Close()) }()

	got := drain(t, op)
	require.Equal(t, []float64{0, 1, 2, 3, 4}, got)
	require.True(t, op.inline, "should have degraded to inline execution")
}

func TestConcurrentClosesChild(t *testing.T) {
	t.Parallel()

	child := &countingOp{n: 2}
	op := newConcurrent(child)

	require.NoError(t, op.Close())
	require.True(t, child.closed, "Close must reach the child")
}

func TestSemaphoreTryAcquireNeverBlocks(t *testing.T) {
	t.Parallel()

	s := newSemaphore(2)

	require.True(t, s.tryAcquire())
	require.True(t, s.tryAcquire())
	require.False(t, s.tryAcquire(), "third acquire must fail rather than block")

	s.release()
	require.True(t, s.tryAcquire())
}
