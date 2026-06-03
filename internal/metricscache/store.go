package metricscache

import (
	"github.com/go-faster/errors"
	"github.com/maypok86/otter"
)

// StoreStats contains cache hit/eviction statistics from the backing store.
type StoreStats struct {
	Hits         int64
	Misses       int64
	EvictedCount int64
	EvictedCost  int64
	RejectedSets int64
	Size         int
	Ratio        float64

	// DiskSize is the approximate total size of on-disk files (.json + .bin)
	// for DiskStore. It is zero for MemoryStore. Useful for monitoring
	// unbounded growth (DiskStore has no eviction in v1).
	DiskSize int64
}

// Store is a pluggable backend for the metrics cache.
type Store interface {
	Get(key Key) (*Entry, bool)
	Set(key Key, entry *Entry)
	Size() int
	Stats() StoreStats
	Close() error
}

// MemoryStore is an in-memory Store backed by otter.
type MemoryStore struct {
	cache    otter.Cache[Key, *Entry]
	maxBytes int64
}

// NewMemoryStore creates an otter-backed in-memory store with the given memory budget.
func NewMemoryStore(maxBytes int64) (*MemoryStore, error) {
	if maxBytes <= 0 {
		return nil, errors.New("maxBytes must be positive")
	}

	builder := otter.MustBuilder[Key, *Entry](int(maxBytes)).
		Cost(func(_ Key, e *Entry) uint32 {
			return e.Cost()
		}).
		WithTTL(cacheTTL)
	builder = builder.CollectStats()

	cache, err := builder.Build()
	if err != nil {
		return nil, errors.Wrap(err, "build otter cache")
	}

	return &MemoryStore{
		cache:    cache,
		maxBytes: maxBytes,
	}, nil
}

// Get retrieves an entry from the memory store.
func (s *MemoryStore) Get(key Key) (*Entry, bool) {
	return s.cache.Get(key)
}

// Set stores an entry in the memory store.
func (s *MemoryStore) Set(key Key, entry *Entry) {
	s.cache.Set(key, entry)
}

// Size returns the current number of entries in the store.
func (s *MemoryStore) Size() int {
	return s.cache.Size()
}

// Stats returns current statistics from the backing otter cache.
func (s *MemoryStore) Stats() StoreStats {
	st := s.cache.Stats()
	return StoreStats{
		Hits:         st.Hits(),
		Misses:       st.Misses(),
		EvictedCount: st.EvictedCount(),
		EvictedCost:  st.EvictedCost(),
		RejectedSets: st.RejectedSets(),
		Size:         s.cache.Size(),
		Ratio:        st.Ratio(),
		DiskSize:     0,
	}
}

// Close is a no-op for the memory store.
func (s *MemoryStore) Close() error {
	s.cache.Close()
	return nil
}
