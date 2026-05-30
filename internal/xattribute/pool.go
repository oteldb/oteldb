package xattribute

import "github.com/oteldb/oteldb/internal/xsync"

type stringSlice struct {
	val []string
}

func (s *stringSlice) Reset() {
	s.val = s.val[:0]
}

var stringSlicePool = xsync.NewPool(func() *stringSlice {
	return &stringSlice{
		val: make([]string, 0, 16),
	}
})
