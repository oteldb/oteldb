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

// Dropped implements Metrics.
func (NoopMetrics) Dropped(context.Context, string) {}

// Sampled implements Metrics.
func (NoopMetrics) Sampled(context.Context) {}

// Compacted implements Metrics.
func (NoopMetrics) Compacted(context.Context) {}

// Collapsed implements Metrics.
func (NoopMetrics) Collapsed(context.Context) {}

// Recorder adapts a concrete record type to the shared safety handler.
type Recorder[R any] interface {
	Key(record R, fields []string) string
	Time(record R) time.Time
	Truncate(slot int64, record R, windowStart, windowEnd time.Time)
	Compact(key string, record R)
}

// Handler applies shared excess handling state for a concrete record type.
type Handler[R any] struct {
	mode              string
	fields            []string
	threshold         int
	maxBuckets        int
	truncateThreshold int
	window            time.Duration
	sample            func() bool
	metrics           Metrics

	order   list.List
	buckets map[string]*bucket
}

type bucket struct {
	key     string
	count   int
	element *list.Element
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
		mode:              cfg.Mode(),
		fields:            cfg.CompactKeyFields,
		threshold:         cfg.CompactThreshold,
		maxBuckets:        cfg.CompactMaxBuckets,
		truncateThreshold: cfg.TruncateThreshold,
		window:            cfg.CompactWindow,
		sample:            sampler,
		metrics:           metrics,
	}
	if h.mode == ModeCompact {
		h.buckets = make(map[string]*bucket, cfg.CompactMaxBuckets)
	}
	return h
}

// Enabled reports whether the handler performs any excess handling.
func (h *Handler[R]) Enabled() bool {
	return h != nil && h.mode != "" && h.mode != ModeConsume
}

// Handle applies excess handling. It returns true when the original record must be dropped.
func (h *Handler[R]) Handle(ctx context.Context, recorder Recorder[R], record R) bool {
	if !h.Enabled() {
		return false
	}

	switch h.mode {
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
	recorder.Truncate(windowStart.UnixNano(), record, windowStart, windowStart.Add(h.window))
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
				delete(h.buckets, oldest.Value.(*bucket).key)
				h.order.Remove(oldest)
			}
			return h.handleSample(ctx)
		}
		b = &bucket{key: key}
		b.element = h.order.PushFront(b)
		h.buckets[key] = b
	} else {
		h.order.MoveToFront(b.element)
	}

	b.count++
	if b.count < h.threshold {
		return false
	}
	if h.truncateThreshold > 0 && b.count > h.truncateThreshold {
		return h.handleTruncate(ctx, recorder, record)
	}

	recorder.Compact(key, record)
	h.metrics.Compacted(ctx)
	h.metrics.Collapsed(ctx)
	return true
}
