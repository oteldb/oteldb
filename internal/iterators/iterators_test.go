package iterators

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyIterator(t *testing.T) {
	a := require.New(t)
	ei := Empty[int]()

	a.NoError(ForEach[int](ei, func(int) error {
		a.Fail("Must not be called")
		return nil
	}))
	a.NoError(ei.Close())
}

func TestSliceIterator(t *testing.T) {
	tests := []struct {
		values []int
	}{
		{[]int{}},
		{[]int{1}},
		{[]int{1, 2, 3}},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			a := require.New(t)
			si := Slice(tt.values)

			got := make([]int, 0, len(tt.values))
			a.NoError(ForEach[int](si, func(v int) error {
				got = append(got, v)
				return nil
			}))
			a.Equal(tt.values, got)

			// To be sure that iterator properly handle Next calls
			// even if there is no elements anymore.
			var d int
			a.False(si.Next(&d))
			a.False(si.Next(&d))
			a.NoError(si.Close())
		})
	}
}
