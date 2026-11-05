// Package memsize estimates owned backing storage; allocator rounding is excluded.
package memsize

import "unsafe"

// MapBase estimates the runtime map header and its first small group.
const MapBase int64 = 512

// Of returns the fixed size of T, excluding referenced storage.
func Of[T any]() int64 {
	var value T
	return int64(unsafe.Sizeof(value))
}

// Slice counts capacity because shortening a slice retains its backing array.
func Slice[T any](values []T) int64 { return int64(cap(values)) * Of[T]() }

// MapEntry estimates one slot with control bytes and growth slack.
func MapEntry[K comparable, V any]() int64 { return 2 * (Of[K]() + Of[V]() + 1) }

// Map estimates a freshly built map; deleted capacity is unobservable.
// Slots, control bytes and growth slack are included; payloads belong to callers.
func Map[K comparable, V any](values map[K]V) int64 {
	if values == nil {
		return 0
	}
	return MapBase + int64(len(values))*MapEntry[K, V]()
}
