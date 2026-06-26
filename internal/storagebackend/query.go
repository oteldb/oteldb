package storagebackend

import (
	"math"
	"time"

	"github.com/oteldb/storage/signal"
)

// fetchWindow converts an optional [start, end] time range into the unix-nanosecond bounds
// expected by a [fetch.Request]. A zero start or end widens that side to the full range, so a
// query without an explicit bound scans everything (mirroring how the metrics adapter clamps the
// Prometheus millisecond window).
func fetchWindow(start, end time.Time) (int64, int64) {
	lo := int64(math.MinInt64)
	if !start.IsZero() {
		lo = start.UnixNano()
	}
	hi := int64(math.MaxInt64)
	if !end.IsZero() {
		hi = end.UnixNano()
	}
	return lo, hi
}

// seriesWindow converts an optional [start, end] range into the (start, end) unix-nanosecond
// arguments of [storage.Storage.ProfileSeries] and similar enumeration primitives, where a zero
// pair disables the time filter. A half-open range keeps the set bound it was given.
func seriesWindow(start, end time.Time) (int64, int64) {
	var lo, hi int64
	if !start.IsZero() {
		lo = start.UnixNano()
	}
	if !end.IsZero() {
		hi = end.UnixNano()
	}
	return lo, hi
}

// valueText renders a typed storage value to its canonical text form, the representation matchers
// and label enumeration compare against.
func valueText(v signal.Value) string {
	return string(v.AppendText(nil))
}
