package storagebackend

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// The config inverts the storage library's polarity so it reads like the cache settings beside it:
// there, an explicit 0 disables. Getting this backwards would silently leave reads unbounded for an
// operator who asked for a limit, or refuse every query for one who asked for none.
func TestResolveMaxQueryBytes(t *testing.T) {
	t.Parallel()

	size := func(n int64) *xbytes.Bytes { b := xbytes.Bytes(n); return &b }

	for _, tc := range []struct {
		name string
		cfg  *xbytes.Bytes
		want int64
	}{
		{"unset defers to the library default", nil, 0},
		{"explicit size is passed through", size(64 << 20), 64 << 20},
		{"explicit zero disables the bound", size(0), -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, resolveCacheSettings(Config{MaxQueryBytes: tc.cfg}).MaxQuery)
		})
	}
}
