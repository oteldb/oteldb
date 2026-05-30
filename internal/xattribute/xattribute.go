// Package xattribute provides some helpers to create OpenTelemetry attributes.
package xattribute

import (
	"fmt"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/oteldb/oteldb/internal/xsync"
)

// StringerSlice creates a string slice attribute from slice of [fmt.Stringer] implementations.
func StringerSlice[S ~[]E, E fmt.Stringer](k string, v S) attribute.KeyValue {
	if len(v) == 0 {
		return attribute.StringSlice(k, nil)
	}

	// Using pooled string slice is fine, since attribute package copies slice.
	ss := xsync.GetReset(stringSlicePool)
	defer stringSlicePool.Put(ss)

	ss.val = append(ss.val, make([]string, len(v))...)
	for i, f := range v {
		ss.val[i] = safeStringer(f)
	}
	return attribute.StringSlice(k, ss.val)
}

func safeStringer[F fmt.Stringer](f F) (v string) {
	defer func() {
		if r := recover(); r != nil {
			v = "<stringer panic>:" + fmt.Sprint(r)
		}
	}()
	return f.String()
}

// StringMap creates a sorted string slice attribute from string map.
func StringMap(k string, m map[string]string) attribute.KeyValue {
	if len(m) == 0 {
		return attribute.StringSlice(k, nil)
	}

	// Using pooled string slice is fine, since attribute package copies slice.
	ss := xsync.GetReset(stringSlicePool)
	defer stringSlicePool.Put(ss)

	for k, v := range m {
		ss.val = append(ss.val, k+"="+v)
	}
	slices.Sort(ss.val)

	return attribute.StringSlice(k, ss.val)
}

// UnixNano returns [time.Time] as unix nano timestamp.
//
// If value is zero, timestamp would be zero too.
func UnixNano(k string, t time.Time) attribute.KeyValue {
	var v int64
	if !t.IsZero() {
		v = t.UnixNano()
	}
	return attribute.Int64(k, v)
}

// Duration returns [time.Duration] as integer attribute.
func Duration(k string, d time.Duration) attribute.KeyValue {
	return attribute.Int64(k, d.Nanoseconds())
}
