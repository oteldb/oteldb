package odbsafety

import (
	"container/list"
	"context"
	"time"
)

// Metrics records safety handler decisions.
type Metrics interface {
	Dropped(context.Context, string)
	Sampled(context.Context)
	Compacted(context.Context)
	Collapsed(context.Context)
}

// NoopMetrics disables safety handler metrics.
type NoopMetrics struct{}

func (NoopMetrics) Dropped(context.Context, string) {}
func (NoopMetrics) Sampled(context.Context)         {}
func (NoopMetrics) Compacted(context.Context)       {}
func (NoopMetrics) Collapsed(context.Context)       {}

// Recorder adapts a concrete record type to the shared safety handler.
type Recorder[R any] interface {
	PassThrough(record R) bool
	Key(record R, fields []string) string
	Time(record R) time.Time
	Clone(record R) R
	Truncate(slot int64, count int, record R, windowStart, windowEnd time.Time)
	Compact(key string, count int, record R)
}

// Handler applies shared excess handling state for a concrete record type.
type Handler[R any] struct {
	fields            []string
	threshold         int
	maxBuckets        int
	truncateThreshold int
	window            time.Duration
	sample            func() bool
	metrics           Metrics
	now               func() time.Time

	order     list.List
	buckets   map[string]*bucket[R]
	truncated map[int64]*truncateBucket[R]
}

type bucket[R any] struct {
	key          string
	count        int
	droppedCount int
	element      *list.Element
	record       R
	lastUpdated  time.Time
}

type truncateBucket[R any] struct {
	count       int
	record      R
	windowStart time.Time
	windowEnd   time.Time
}

// NewHandler creates a shared excess handler.
func NewHandler[R any](cfg Config, sampler func() bool, metrics Metrics) *Handler[R] {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	if sampler == nil {
		sampler = func() bool { return false }
	}
	h := &Handler[R]{
		fields:            cfg.CompactKeyFields,
		threshold:         cfg.CompactThreshold,
		maxBuckets:        cfg.CompactMaxBuckets,
		truncateThreshold: cfg.TruncateThreshold,
		window:            cfg.CompactWindow,
		sample:            sampler,
		metrics:           metrics,
		now:               time.Now,
		buckets:           make(map[string]*bucket[R], cfg.CompactMaxBuckets),
		truncated:         make(map[int64]*truncateBucket[R]),
	}
	return h
}

// SetNow overrides the current time function for testing.
func (h *Handler[R]) SetNow(now func() time.Time) {
	if h != nil {
		h.now = now
	}
}

// BucketCount returns the number of active compact buckets.
func (h *Handler[R]) BucketCount() int {
	if h == nil {
		return 0
	}
	return len(h.buckets)
}

// Handle applies excess handling. It returns true when the original record must be dropped.
func (h *Handler[R]) Handle(ctx context.Context, mode string, recorder Recorder[R], record R) bool {
	if h == nil || mode == "" || mode == ModeConsume || recorder.PassThrough(record) {
		return false
	}

	switch mode {
	case ModeDrop:
		h.metrics.Dropped(ctx, "rate_limit")
		return true
	case ModeSample:
		return h.handleSample(ctx)
	case ModeTruncate:
		return h.handleTruncate(ctx, recorder, record)
	case ModeCompact:
		return h.handleCompact(ctx, recorder, record)
	default:
		return false
	}
}

func (h *Handler[R]) handleSample(ctx context.Context) bool {
	if h.sample() {
		h.metrics.Sampled(ctx)
		return false
	}
	h.metrics.Dropped(ctx, "sample")
	return true
}

func (h *Handler[R]) handleTruncate(ctx context.Context, recorder Recorder[R], record R) bool {
	if h.window <= 0 {
		h.metrics.Dropped(ctx, "truncate")
		return true
	}
	windowStart := recorder.Time(record).Truncate(h.window)
	slot := windowStart.UnixNano()

	b, ok := h.truncated[slot]
	if !ok {
		b = &truncateBucket[R]{
			record:      recorder.Clone(record),
			windowStart: windowStart,
			windowEnd:   windowStart.Add(h.window),
		}
		h.truncated[slot] = b
	}
	b.count++
	h.metrics.Dropped(ctx, "truncate")
	return true
}

func (h *Handler[R]) handleCompact(ctx context.Context, recorder Recorder[R], record R) bool {
	key := recorder.Key(record, h.fields)
	b, ok := h.buckets[key]
	if !ok {
		if len(h.buckets) >= h.maxBuckets {
			oldest := h.order.Back()
			if oldest != nil {
				// Flush it before deleting if it has dropped counts
				oldB := oldest.Value.(*bucket[R])
				if oldB.droppedCount > 0 {
					recorder.Compact(oldB.key, oldB.droppedCount, oldB.record)
					h.metrics.Compacted(ctx)
				}
				delete(h.buckets, oldB.key)
				h.order.Remove(oldest)
			}
			return h.handleSample(ctx)
		}
		b = &bucket[R]{key: key, record: recorder.Clone(record)}
		b.element = h.order.PushFront(b)
		h.buckets[key] = b
	} else {
		h.order.MoveToFront(b.element)
	}

	b.count++
	b.lastUpdated = h.now()

	if b.count < h.threshold {
		return false
	}
	if h.truncateThreshold > 0 && b.count > h.truncateThreshold {
		return h.handleTruncate(ctx, recorder, record)
	}

	// Drop the record, accumulate dropped count
	b.droppedCount++
	h.metrics.Collapsed(ctx)
	return true
}

// Flush emits synthetic records for accumulated drops and cleans up stale state.
func (h *Handler[R]) Flush(ctx context.Context, recorder Recorder[R]) {
	if h == nil {
		return
	}
	now := h.now()

	// Flush and cleanup truncated slots that are past their window.
	for slot, b := range h.truncated {
		if now.After(b.windowEnd) {
			recorder.Truncate(slot, b.count, b.record, b.windowStart, b.windowEnd)
			delete(h.truncated, slot)
		}
	}

	// Flush compacted records and cleanup stale buckets
	var next *list.Element
	for e := h.order.Front(); e != nil; e = next {
		next = e.Next()
		b := e.Value.(*bucket[R])

		// Emit if we dropped anything
		if b.droppedCount > 0 {
			recorder.Compact(b.key, b.droppedCount, b.record)
			h.metrics.Compacted(ctx)
			b.droppedCount = 0
		}

		// Delete stale buckets
		if h.window > 0 && now.Sub(b.lastUpdated) > h.window {
			delete(h.buckets, b.key)
			h.order.Remove(e)
		}
	}
}
