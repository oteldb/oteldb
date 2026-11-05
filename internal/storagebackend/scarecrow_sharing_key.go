package storagebackend

import (
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"time"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/zeebo/xxh3"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func nativeKey(operation string, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) nativeQueryKey {
	var stack [16]*labels.Matcher
	ordered := stack[:0]
	if len(matchers) > len(stack) {
		ordered = make([]*labels.Matcher, 0, len(matchers))
	}
	ordered = append(ordered, matchers...)
	slices.SortFunc(ordered, func(a, b *labels.Matcher) int {
		return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Value, b.Value))
	})
	var hasher xxh3.Hasher
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(ordered)))
	_, _ = hasher.Write(length[:])
	for _, m := range ordered {
		_, _ = hasher.Write([]byte{byte(m.Type)})
		binary.LittleEndian.PutUint64(length[:], uint64(len(m.Name)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.WriteString(m.Name)
		binary.LittleEndian.PutUint64(length[:], uint64(len(m.Value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.WriteString(m.Value)
	}
	return nativeQueryKey{operation: operation, grid: grid, label: label, matchers: hasher.Sum128()}
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
