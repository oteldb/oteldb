package storagebackend

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/oteldb/internal/memsize"
)

// This conservatively covers Otter's node links, counters and queue references.
const nativeCacheNodeOverhead = 64

func nativeCacheCost(key nativeQueryKey, payload int64) int64 {
	return payload + int64(len(key.label)+len(key.tenant)) + memsize.Of[nativeCacheEntry]() + memsize.Of[nativeQueryKey]() + memsize.MapEntry[uint64, uintptr]() + nativeCacheNodeOverhead
}

func labelPayloadBytes(ls labels.Labels) int64 {
	var n int64
	ls.Range(func(l labels.Label) { n += int64(len(l.Name)+len(l.Value)) + memsize.Of[labels.Label]() })
	return n
}
