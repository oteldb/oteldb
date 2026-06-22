package odbsafety

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stringRec is a minimal record type for handler tests.
type stringRec string

// stringRecorder implements Recorder[stringRec].
type stringRecorder struct {
	truncated []string
	compacted map[string]int
}

func (r *stringRecorder) PassThrough(_ stringRec) bool { return false }

func (r *stringRecorder) Key(record stringRec, _ []string) string { return string(record) }

func (r *stringRecorder) Time(_ stringRec) time.Time { return time.Unix(100, 0).UTC() }

func (r *stringRecorder) Clone(record stringRec) stringRec { return record }

func (r *stringRecorder) Truncate(_ int64, count int, record stringRec, _, _ time.Time) {
	for range count {
		r.truncated = append(r.truncated, string(record))
	}
}

func (r *stringRecorder) Compact(key string, count int, _ stringRec) {
	if r.compacted == nil {
		r.compacted = make(map[string]int)
	}
	r.compacted[key] += count
}

func newTestHandler(cfg Config) *Handler[stringRec] {
	sampler := func() bool { return false }
	return NewHandler[stringRec](cfg, sampler, NoopMetrics{})
}

func TestHandlerDrop(t *testing.T) {
	h := newTestHandler(Config{OnExcess: ModeDrop})
	r := &stringRecorder{}
	require.True(t, h.Handle(context.Background(), ModeDrop, r, "msg"))
	h.Flush(context.Background(), r)
	require.Empty(t, r.truncated)
	require.Empty(t, r.compacted)
}

func TestHandlerSampleDrop(t *testing.T) {
	h := NewHandler[stringRec](Config{OnExcess: ModeSample}, func() bool { return false }, NoopMetrics{})
	r := &stringRecorder{}
	require.True(t, h.Handle(context.Background(), ModeSample, r, "msg"))
}

func TestHandlerSampleKeep(t *testing.T) {
	h := NewHandler[stringRec](Config{OnExcess: ModeSample}, func() bool { return true }, NoopMetrics{})
	r := &stringRecorder{}
	require.False(t, h.Handle(context.Background(), ModeSample, r, "msg"))
}

func TestHandlerTruncate(t *testing.T) {
	h := newTestHandler(Config{
		OnExcess:      ModeTruncate,
		CompactWindow: 30 * time.Second,
	})
	// Mock time
	h.now = func() time.Time { return time.Unix(200, 0) }

	r := &stringRecorder{}
	require.True(t, h.Handle(context.Background(), ModeTruncate, r, "msg"))
	h.Flush(context.Background(), r)
	require.Len(t, r.truncated, 1)
	require.Empty(t, r.compacted)
}

func TestHandlerCompactBelowThreshold(t *testing.T) {
	h := newTestHandler(Config{
		OnExcess:          ModeCompact,
		CompactThreshold:  3,
		CompactMaxBuckets: 10,
		CompactWindow:     30 * time.Second,
	})
	r := &stringRecorder{}
	// count=1 < threshold → not dropped
	require.False(t, h.Handle(context.Background(), ModeCompact, r, "a"))
	require.Equal(t, 1, h.BucketCount())
}

func TestHandlerCompactAtThreshold(t *testing.T) {
	h := newTestHandler(Config{
		OnExcess:          ModeCompact,
		CompactThreshold:  2,
		CompactMaxBuckets: 10,
		CompactWindow:     30 * time.Second,
	})
	r := &stringRecorder{}
	ctx := context.Background()
	require.False(t, h.Handle(ctx, ModeCompact, r, "a")) // count=1 → pass
	require.True(t, h.Handle(ctx, ModeCompact, r, "a"))  // count=2 → compact
	h.Flush(ctx, r)
	require.Equal(t, 1, r.compacted["a"])
}

func TestHandlerCompactEscalatesToTruncate(t *testing.T) {
	h := newTestHandler(Config{
		OnExcess:          ModeCompact,
		CompactThreshold:  2,
		CompactMaxBuckets: 10,
		CompactWindow:     30 * time.Second,
		TruncateThreshold: 3,
	})
	// Mock time so flush works
	h.now = func() time.Time { return time.Unix(200, 0) }

	r := &stringRecorder{}
	ctx := context.Background()
	require.False(t, h.Handle(ctx, ModeCompact, r, "a")) // count=1 → pass
	require.True(t, h.Handle(ctx, ModeCompact, r, "a"))  // count=2 → compact
	require.True(t, h.Handle(ctx, ModeCompact, r, "a"))  // count=3 → compact (3 not > 3)
	require.True(t, h.Handle(ctx, ModeCompact, r, "a"))  // count=4 → truncate (4 > 3)
	h.Flush(ctx, r)
	require.Equal(t, 2, r.compacted["a"])
	require.Len(t, r.truncated, 1)
}

func TestHandlerCompactLRUOverflow(t *testing.T) {
	h := NewHandler[stringRec](Config{
		OnExcess:          ModeCompact,
		CompactThreshold:  100,
		CompactMaxBuckets: 2,
		CompactWindow:     30 * time.Second,
	}, func() bool { return false }, NoopMetrics{})
	r := &stringRecorder{}
	ctx := context.Background()

	// Fill LRU to capacity.
	require.False(t, h.Handle(ctx, ModeCompact, r, "a"))
	require.False(t, h.Handle(ctx, ModeCompact, r, "b"))
	require.Equal(t, 2, h.BucketCount())

	// Third distinct key → evict oldest, degrade to sample (rate=0 → drop).
	// New key is not inserted — handler returns early via the sample path.
	require.True(t, h.Handle(ctx, ModeCompact, r, "c"))
	require.Equal(t, 1, h.BucketCount()) // evicted one, new key not inserted
}

func TestHandlerConsumeIsNoop(t *testing.T) {
	h := newTestHandler(Config{OnExcess: ModeConsume})
	r := &stringRecorder{}
	// Handle should not be called in consume mode, but guard just in case.
	require.False(t, h.Handle(context.Background(), ModeConsume, r, "msg"))
}

func TestHandlerNilIsDisabled(t *testing.T) {
	var h *Handler[stringRec]
	require.False(t, h.Handle(context.Background(), ModeConsume, nil, "msg"))
	require.Equal(t, 0, h.BucketCount())
}

func TestHandlerTruncateZeroWindow(t *testing.T) {
	h := newTestHandler(Config{OnExcess: ModeTruncate, CompactWindow: 0})
	r := &stringRecorder{}
	// Zero window → drop immediately without creating a truncation record.
	require.True(t, h.Handle(context.Background(), ModeTruncate, r, "msg"))
	h.Flush(context.Background(), r)
	require.Empty(t, r.truncated)
}
