package storagebackend

import (
	"container/list"
	"context"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

const defaultNativeQueryCacheBytes = 64 << 20

// WithNativeQuerySharing bounds concurrent native operations and their shared result size.
// Zero disables sharing and completed caching. A custom Source must authorize
// reads before this seam and return the same data for the same explicit request arguments.
func WithNativeQuerySharing(maxOperations int, maxResultBytes int64) Option {
	return func(b *Backend) {
		b.disableSharing = true
		b.sharing = newNativeQuerySharing(maxOperations, maxResultBytes)
	}
}

// WithNativeQueryCache bounds completed native results. Zero disables completed caching.
// Reuse requires a source revision and never extends its age-retention deadline.
func WithNativeQueryCache(bytes int64) Option {
	return func(b *Backend) { b.nativeCacheBytes = max(bytes, 0) }
}

type nativeCacheEntry struct {
	key   nativeQueryKey
	value any
	bytes int64
}

type nativeQuerySharing struct {
	producers      map[*fetch.Scope]int
	cache          map[nativeQueryKey]*list.Element
	lru            list.List
	cacheBytes     int64
	maxCacheBytes  int64
	mu             sync.Mutex
	calls          map[nativeQueryKey]*nativeQueryCall
	maxOperations  int
	active         int
	maxResultBytes int64
}

type nativeQueryKey struct {
	tenant            signal.TenantID
	revision          storage.MetricReadRevision
	cacheable         bool
	revisionAvailable bool
	operation         string
	matchers          string
	label             string
	grid              scarecrow.WindowGrid
}

type nativeQueryCall struct {
	scope     *fetch.Scope
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	value     any
	err       error
	oversized bool
}

func newNativeQuerySharing(operations int, resultBytes int64) *nativeQuerySharing {
	if operations <= 0 || resultBytes <= 0 {
		return nil
	}
	return &nativeQuerySharing{producers: make(map[*fetch.Scope]int), cache: make(map[nativeQueryKey]*list.Element), calls: make(map[nativeQueryKey]*nativeQueryCall), maxOperations: operations, maxResultBytes: resultBytes}
}

func nativeKey(operation string, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) nativeQueryKey {
	parts := make([]string, len(matchers))
	for i, m := range matchers {
		parts[i] = strconv.Itoa(int(m.Type)) + ":" + strconv.Quote(m.Name) + ":" + strconv.Quote(m.Value)
	}
	slices.Sort(parts)
	return nativeQueryKey{operation: operation, grid: grid, label: label, matchers: strings.Join(parts, ";")}
}

func (s *scarecrowScanner) sharingKey(operation string, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) nativeQueryKey {
	key := nativeKey(operation, grid, label, matchers)
	key.tenant = s.b.tenant
	if s.b.sharing.maxCacheBytes <= 0 && operation != "series" {
		return key
	}
	source, ok := s.b.src.(interface {
		MetricReadRevision(signal.TenantID, int64) (storage.MetricReadRevision, bool)
	})
	if !ok {
		return key
	}
	start := grid.Start
	switch operation {
	case "aggregate-grid", "count-grid":
		if grid.Width <= 0 || start < math.MinInt64+grid.Width {
			return key
		}
		start = start - grid.Width + 1
	case "series":
		if start == 0 && grid.Width == 0 {
			start = math.MinInt64
		}
	default:
		if start == math.MaxInt64 {
			return key
		}
		start++
	}
	const nsPerMs = int64(time.Millisecond)
	if start < math.MinInt64/nsPerMs || start > math.MaxInt64/nsPerMs {
		return key
	}
	key.revision, key.revisionAvailable = source.MetricReadRevision(s.b.tenant, start*nsPerMs)
	key.cacheable = key.revisionAvailable && s.b.sharing.maxCacheBytes > 0
	return key
}

func (g *nativeQuerySharing) put(key nativeQueryKey, value any, bytes int64) {
	for g.cacheBytes > g.maxCacheBytes-bytes {
		oldest := g.lru.Back()
		entry := oldest.Value.(*nativeCacheEntry)
		delete(g.cache, entry.key)
		g.cacheBytes -= entry.bytes
		g.lru.Remove(oldest)
	}
	entry := &nativeCacheEntry{key: key, value: value, bytes: bytes}
	g.cache[key] = g.lru.PushFront(entry)
	g.cacheBytes += bytes
}

func (s *scarecrowScanner) sharingContext(ctx context.Context) context.Context {
	// Producer accounting is per Backend, so callers cannot rebind this scanner's private scope.
	if fetch.ScopeFrom(ctx) == s.scope {
		return ctx
	}
	return fetch.WithScope(ctx, s.scope)
}

// shareNative never queues for admission: saturation and oversized results fall back to a read.
// A flight borrows its first query's scope only until the native call finishes. Replacing that
// scope can deadlock against iterators the same query already owns under decode admission.
func shareNative[T any](ctx context.Context, group *nativeQuerySharing, key nativeQueryKey,
	compute func(context.Context) (T, error), size func(T) int64, clone func(T) T,
) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if group == nil || key.operation == "series" && !key.revisionAvailable {
		return compute(ctx)
	}
	if key.revision.ValidUntil != 0 && time.Now().UnixNano() >= key.revision.ValidUntil {
		key.cacheable = false
	}
	flightKey := key
	if key.operation != "series" {
		flightKey.revision = storage.MetricReadRevision{}
	}
	flightKey.cacheable = false
	flightKey.revisionAvailable = false
	group.mu.Lock()
	if entry, hit := group.cache[key]; key.cacheable && hit {
		group.lru.MoveToFront(entry)
		value := entry.Value.(*nativeCacheEntry).value.(T)
		group.mu.Unlock()
		return clone(value), nil
	}
	call, found := group.calls[flightKey]
	var releaseScope func()
	if found {
		scope := fetch.ScopeFrom(ctx)
		if scope != nil && scope != call.scope {
			if group.producers[scope] != 0 {
				group.mu.Unlock()
				return compute(ctx)
			}
			holding, inspected := scope.TryEnter()
			if !inspected {
				group.mu.Unlock()
				return compute(ctx)
			}
			if holding {
				scope.Abort()
				group.mu.Unlock()
				return compute(ctx)
			}
			// Keep this empty scope excluded until the wait ends: another branch must not
			// acquire the quota this caller's shared producer is waiting for.
			releaseScope = scope.Abort
		}
	}
	defer func() {
		if releaseScope != nil {
			releaseScope()
		}
	}()
	if !found && group.active >= group.maxOperations {
		group.mu.Unlock()
		return compute(ctx)
	}
	if !found {
		workCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		call = &nativeQueryCall{done: make(chan struct{}), cancel: cancel, scope: fetch.ScopeFrom(ctx)}
		group.calls[flightKey] = call
		group.active++
		if call.scope != nil {
			group.producers[call.scope]++
		}
		go func() {
			value, err := compute(workCtx)
			var bytes int64
			if err == nil {
				bytes = size(value) + int64(len(key.matchers)+len(key.label)+len(key.tenant)) + 2*int64(unsafe.Sizeof(nativeCacheEntry{})) + int64(unsafe.Sizeof(list.Element{}))
			}
			group.mu.Lock()
			group.active--
			if call.scope != nil {
				group.producers[call.scope]--
				if group.producers[call.scope] == 0 {
					delete(group.producers, call.scope)
				}
			}
			call.value, call.err = value, err
			call.oversized = err == nil && bytes > group.maxResultBytes
			unexpired := key.revision.ValidUntil == 0 || time.Now().UnixNano() < key.revision.ValidUntil
			if err == nil && !call.oversized && key.cacheable && unexpired && workCtx.Err() == nil && bytes <= group.maxCacheBytes {
				group.put(key, value, bytes)
			}
			if group.calls[flightKey] == call {
				delete(group.calls, flightKey)
			}
			close(call.done)
			group.mu.Unlock()
			cancel()
		}()
	}
	call.waiters++
	group.mu.Unlock()
	defer func() {
		group.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			call.cancel()
			if group.calls[flightKey] == call {
				delete(group.calls, flightKey)
			}
		}
		group.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-call.done:
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if call.err != nil {
		return zero, call.err
	}
	value := call.value.(T)
	if call.oversized {
		if found {
			if releaseScope != nil {
				releaseScope()
				releaseScope = nil
			}
			return compute(ctx)
		}
		return value, nil
	}
	return clone(value), nil
}

func labelsBytes(ls labels.Labels) int64 {
	n := int64(unsafe.Sizeof(ls))
	ls.Range(func(l labels.Label) { n += int64(len(l.Name)+len(l.Value)) + int64(unsafe.Sizeof(l)) })
	return n
}

func (s *scarecrowScanner) Series(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]labels.Labels, error) {
	if s.b.sharing == nil {
		return s.series(ctx, mint, maxt, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("series", scarecrow.WindowGrid{Start: mint, Width: maxt}, "", matchers),
		func(ctx context.Context) ([]labels.Labels, error) { return s.series(ctx, mint, maxt, matchers) },
		func(values []labels.Labels) (n int64) {
			n += int64(cap(values)) * int64(unsafe.Sizeof(labels.Labels{}))
			for _, ls := range values {
				n += labelsBytes(ls)
			}
			return n
		},
		func(values []labels.Labels) []labels.Labels {
			out := make([]labels.Labels, len(values))
			for i, ls := range values {
				out[i] = ls.Copy()
			}
			return out
		})
}

func (s *scarecrowScanner) AggregateGrid(ctx context.Context, grid scarecrow.WindowGrid, matchers []*labels.Matcher) ([]scarecrow.GridAggregate, error) {
	if s.b.sharing == nil {
		return s.aggregateGrid(ctx, grid, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("aggregate-grid", grid, "", matchers),
		func(ctx context.Context) ([]scarecrow.GridAggregate, error) {
			return s.aggregateGrid(ctx, grid, matchers)
		},
		func(values []scarecrow.GridAggregate) (n int64) {
			n += int64(cap(values)) * int64(unsafe.Sizeof(scarecrow.GridAggregate{}))
			for _, value := range values {
				n += labelsBytes(value.Labels) + int64(unsafe.Sizeof(value)) + int64(cap(value.Windows))*int64(unsafe.Sizeof(scarecrow.Aggregate{}))
			}
			return n
		},
		func(values []scarecrow.GridAggregate) []scarecrow.GridAggregate {
			out := slices.Clone(values)
			for i := range out {
				out[i].Labels = out[i].Labels.Copy()
				out[i].Windows = slices.Clone(out[i].Windows)
			}
			return out
		})
}

func (s *scarecrowScanner) AggregateOverTime(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]scarecrow.WindowAggregate, error) {
	if s.b.sharing == nil {
		return s.aggregateOverTime(ctx, mint, maxt, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("aggregate", scarecrow.WindowGrid{Start: mint, Width: maxt}, "", matchers),
		func(ctx context.Context) ([]scarecrow.WindowAggregate, error) {
			return s.aggregateOverTime(ctx, mint, maxt, matchers)
		},
		func(values []scarecrow.WindowAggregate) (n int64) {
			n += int64(cap(values)) * int64(unsafe.Sizeof(scarecrow.WindowAggregate{}))
			for _, value := range values {
				n += labelsBytes(value.Labels) + int64(unsafe.Sizeof(value))
			}
			return n
		},
		func(values []scarecrow.WindowAggregate) []scarecrow.WindowAggregate {
			out := slices.Clone(values)
			for i := range out {
				out[i].Labels = out[i].Labels.Copy()
			}
			return out
		})
}

func (s *scarecrowScanner) CountSeries(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (uint64, error) {
	if s.b.sharing == nil {
		return s.countSeries(ctx, mint, maxt, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("count", scarecrow.WindowGrid{Start: mint, Width: maxt}, "", matchers),
		func(ctx context.Context) (uint64, error) { return s.countSeries(ctx, mint, maxt, matchers) },
		func(uint64) int64 { return 8 }, func(value uint64) uint64 { return value })
}

func (s *scarecrowScanner) CountSeriesBy(ctx context.Context, mint, maxt int64, label string, matchers []*labels.Matcher) (map[string]uint64, error) {
	if s.b.sharing == nil {
		return s.countSeriesBy(ctx, mint, maxt, label, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("count-by", scarecrow.WindowGrid{Start: mint, Width: maxt}, label, matchers),
		func(ctx context.Context) (map[string]uint64, error) {
			return s.countSeriesBy(ctx, mint, maxt, label, matchers)
		},
		func(values map[string]uint64) (n int64) {
			n = 512
			for key := range values {
				n += int64(len(key)) + 128
			}
			return n
		},
		maps.Clone)
}

func (s *scarecrowScanner) CountSeriesGrid(ctx context.Context, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) (map[string][]uint64, error) {
	if s.b.sharing == nil {
		return s.countSeriesGrid(ctx, grid, label, matchers)
	}
	return shareNative(s.sharingContext(ctx), s.b.sharing, s.sharingKey("count-grid", grid, label, matchers),
		func(ctx context.Context) (map[string][]uint64, error) {
			return s.countSeriesGrid(ctx, grid, label, matchers)
		},
		func(values map[string][]uint64) (n int64) {
			n = 512
			for key, value := range values {
				n += int64(len(key)) + int64(cap(value))*8 + 128
			}
			return n
		},
		func(values map[string][]uint64) map[string][]uint64 {
			out := make(map[string][]uint64, len(values))
			for key, value := range values {
				out[key] = slices.Clone(value)
			}
			return out
		})
}
