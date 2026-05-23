package chstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeekIterator(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50}
	step := int64(10)

	tests := []struct {
		seek     int64
		startIdx int
		wantIdx  int
		wantOK   bool
	}{
		{seek: 0, startIdx: -1, wantIdx: 0, wantOK: true},
		{seek: 10, startIdx: -1, wantIdx: 0, wantOK: true},
		{seek: 15, startIdx: -1, wantIdx: 1, wantOK: true},
		{seek: 20, startIdx: -1, wantIdx: 1, wantOK: true},
		{seek: 25, startIdx: -1, wantIdx: 2, wantOK: true},
		{seek: 50, startIdx: -1, wantIdx: 4, wantOK: true},
		{seek: 51, startIdx: -1, wantIdx: 5, wantOK: false},

		// Sequential seek tests
		{seek: 10, startIdx: 0, wantIdx: 0, wantOK: true},
		{seek: 20, startIdx: 0, wantIdx: 1, wantOK: true},
		{seek: 30, startIdx: 1, wantIdx: 2, wantOK: true},
		{seek: 50, startIdx: 2, wantIdx: 4, wantOK: true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i), func(t *testing.T) {
			n := tt.startIdx
			ok := seekIterator(ts, &n, step, tt.seek)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantIdx, n)
		})
	}
}

func TestSeekIterator_Empty(t *testing.T) {
	var ts []int64
	n := -1
	ok := seekIterator(ts, &n, 10, 10)
	require.False(t, ok)
	require.Equal(t, 0, n)
}

func TestInterpolationSeek(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50}
	step := int64(10)

	tests := []struct {
		seek    int64
		wantIdx int
	}{
		{seek: 0, wantIdx: 0},
		{seek: 10, wantIdx: 0},
		{seek: 15, wantIdx: 1},
		{seek: 20, wantIdx: 1},
		{seek: 25, wantIdx: 2},
		{seek: 45, wantIdx: 4},
		{seek: 50, wantIdx: 4},
		{seek: 51, wantIdx: 5},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Seek%d", tt.seek), func(t *testing.T) {
			got := interpolationSeek(ts, step, tt.seek)
			require.Equal(t, tt.wantIdx, got)
		})
	}
}
