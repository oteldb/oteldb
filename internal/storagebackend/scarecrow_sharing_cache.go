package storagebackend

import (
	"math"
	"runtime"
	"strings"

	"github.com/maypok86/otter"
)

func (g *nativeQuerySharing) cacheResult(key nativeQueryKey, value any, bytes int64) {
	if cache := g.completedCache(); cache != nil {
		// A grouping label can be a substring of the whole parser input.
		key.label = strings.Clone(key.label)
		cache.Set(key, &nativeCacheEntry{value: value, bytes: bytes})
	}
}

func (g *nativeQuerySharing) completedCache() *otter.Cache[nativeQueryKey, *nativeCacheEntry] {
	if g.closed || g.maxCacheBytes < nativeCacheCost(nativeQueryKey{}, 0) {
		return nil
	}
	if g.cache == nil {
		capacity := int(min(g.maxCacheBytes, int64(math.MaxInt)))
		cache, err := otter.MustBuilder[nativeQueryKey, *nativeCacheEntry](capacity).InitialCapacity(16).
			Cost(func(_ nativeQueryKey, entry *nativeCacheEntry) uint32 { return uint32(entry.bytes) }).Build()
		if err != nil {
			return nil
		}
		g.cache = &cache
		g.cleanup = runtime.AddCleanup(g, func(cache otter.Cache[nativeQueryKey, *nativeCacheEntry]) { cache.Close() }, cache)
	}
	return g.cache
}

func (g *nativeQuerySharing) closeCache() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	g.cleanup.Stop()
	if g.cache != nil {
		g.cache.Close()
		g.cache = nil
	}
}

// CloseQueryCache stops the native cache worker. The caller must first stop serving queries.
func (b *Backend) CloseQueryCache() { b.sharing.closeCache() }
