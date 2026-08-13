package storagebackend

import (
	"context"
	"io"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// countingIterator reports how many batches were pulled, which is what distinguishes a streaming
// scan from a drained one.
type countingIterator struct {
	batches []*fetch.Batch
	pulled  int
	closed  bool
}

func (it *countingIterator) Next(context.Context) (*fetch.Batch, error) {
	if it.pulled >= len(it.batches) {
		return nil, io.EOF
	}

	b := it.batches[it.pulled]
	it.pulled++

	return b, nil
}

func (it *countingIterator) Close() error {
	it.closed = true

	return nil
}

func testBatch(name string, n int) *fetch.Batch {
	b := &fetch.Batch{
		Series: signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
			Key:   []byte("__name__"),
			Value: signal.StringValue([]byte(name)),
		})},
		Timestamps: make([]int64, n),
		Values:     make([]float64, n),
	}

	for i := range n {
		b.Timestamps[i] = int64(i+1) * int64(1_000_000) // 1ms apart, in nanoseconds.
		b.Values[i] = float64(i)
	}

	return b
}

// TestBatchIteratorStreams is the regression guard for the OOM: a `{__name__=~".+"}`-shaped scan
// used to call fetch.Drain, materializing every matching series before the engine saw the first
// one — so the query's sample budget could never reject it and the process died instead. The
// iterator must pull exactly one batch per Next.
func TestBatchIteratorStreams(t *testing.T) {
	t.Parallel()

	src := &countingIterator{batches: []*fetch.Batch{
		testBatch("a", 3), testBatch("b", 3), testBatch("c", 3),
	}}

	it := &batchIterator{it: src}
	defer func() { require.NoError(t, it.Close()) }()

	require.Zero(t, src.pulled, "constructing the iterator must not read anything")

	s, err := it.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, 1, src.pulled, "one Next must pull exactly one batch, not drain")
	assert.Equal(t, "a", s.Labels.Get("__name__"))
	assert.Equal(t, []int64{1, 2, 3}, s.T, "timestamps convert ns to ms")

	s, err = it.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, 2, src.pulled)
	assert.Equal(t, "b", s.Labels.Get("__name__"))
}

// TestBatchIteratorAbandonedEarly covers what the budget actually does: reject partway and close.
// The remainder must never be read.
func TestBatchIteratorAbandonedEarly(t *testing.T) {
	t.Parallel()

	src := &countingIterator{batches: []*fetch.Batch{
		testBatch("a", 2), testBatch("b", 2), testBatch("c", 2),
	}}

	it := &batchIterator{it: src}

	_, err := it.Next(context.Background())
	require.NoError(t, err)

	require.NoError(t, it.Close())
	assert.Equal(t, 1, src.pulled, "abandoning the scan must leave the rest unread")
	assert.True(t, src.closed, "closing the scan must close the underlying fetch iterator")
}

// TestBatchIteratorSkipsNonMatching pins that the re-check of the unpushable matchers happens per
// batch as it streams, rather than over a drained slice.
func TestBatchIteratorSkipsNonMatching(t *testing.T) {
	t.Parallel()

	src := &countingIterator{batches: []*fetch.Batch{
		testBatch("skip", 1), testBatch("keep", 1),
	}}

	it := &batchIterator{
		it:       src,
		matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "skip")},
	}
	defer func() { require.NoError(t, it.Close()) }()

	s, err := it.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "keep", s.Labels.Get("__name__"))
}

func TestBatchIteratorExhausts(t *testing.T) {
	t.Parallel()

	src := &countingIterator{batches: []*fetch.Batch{testBatch("a", 1)}}

	it := &batchIterator{it: src}

	_, err := it.Next(context.Background())
	require.NoError(t, err)

	s, err := it.Next(context.Background())
	require.NoError(t, err)
	assert.Nil(t, s, "exhaustion reports a nil series, not io.EOF")

	require.NoError(t, it.Close())
}

// TestScarecrowScannerScopePerQuery pins the session boundary. The engine builds one scanner per
// query execution and closes it with the query, so the scanner is what identifies a query to
// storage's admission control. Hoisting the scope onto the Backend would make every query one
// session and quietly delete the decode ceiling; leaving it nil brings back the deadlock, where a
// query holding several reads open blocks against its own reservation.
func TestScarecrowScannerScopePerQuery(t *testing.T) {
	t.Parallel()

	b := &Backend{}

	first, ok := b.ScarecrowScanner().(*scarecrowScanner)
	require.True(t, ok)

	second, ok := b.ScarecrowScanner().(*scarecrowScanner)
	require.True(t, ok)

	require.NotNil(t, first.scope, "a scanner without a scope deadlocks on its second read")
	assert.NotSame(t, first.scope, second.scope, "two queries must not share one session")
}
