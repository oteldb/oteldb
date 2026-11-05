package memsize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedStorage(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		got, want int64
	}{
		{"byte", Of[byte](), 1},
		{"int64", Of[int64](), 8},
		{"slice-capacity", Slice(make([]int64, 1, 10)), 80},
		{"nil-slice", Slice[int64](nil), 0},
		{"nil-map", Map[string, int](nil), 0},
		{"empty-map", Map(map[string]int{}), MapBase},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, tt.got) })
	}
	require.Greater(t, Map(map[string]int{"one": 1}), Map(map[string]int{}))
}
