package scarecrow

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertBounded(t *testing.T) {
	push := func(k int, evictable func(cur, cand float64) bool, vs ...float64) []float64 {
		var cell []limitEntry
		for i, v := range vs {
			insertBounded(&cell, limitEntry{ref: SeriesRef(i), v: v}, k, evictable)
		}

		out := make([]float64, len(cell))
		for i, e := range cell {
			out[i] = e.v
		}

		return out
	}

	t.Run("topk keeps the k largest", func(t *testing.T) {
		got := push(2, topkEvictable, 1, 5, 3, 9, 2)
		require.ElementsMatch(t, []float64{5, 9}, got)
	})

	t.Run("bottomk keeps the k smallest", func(t *testing.T) {
		got := push(2, bottomkEvictable, 1, 5, 3, 9, 2)
		require.ElementsMatch(t, []float64{1, 2}, got)
	})

	t.Run("k larger than input keeps everything", func(t *testing.T) {
		got := push(10, topkEvictable, 1, 5, 3)
		require.ElementsMatch(t, []float64{1, 5, 3}, got)
	})

	t.Run("k<=0 keeps nothing", func(t *testing.T) {
		got := push(0, topkEvictable, 1, 5, 3)
		require.Empty(t, got)
	})

	t.Run("topk evicts a kept NaN once a real value arrives", func(t *testing.T) {
		got := push(1, topkEvictable, math.NaN(), 5)
		require.Equal(t, []float64{5}, got)
	})

	t.Run("topk keeps NaN when nothing better has arrived", func(t *testing.T) {
		cell := push(1, topkEvictable, math.NaN())
		require.Len(t, cell, 1)
		require.True(t, math.IsNaN(cell[0]))
	})
}

func TestQuantile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		q      float64
		values []float64
		want   float64
	}{
		{"empty", 0.5, nil, math.NaN()},
		{"NaN q", math.NaN(), []float64{1, 2, 3}, math.NaN()},
		{"below range", -0.5, []float64{1, 2, 3}, math.Inf(-1)},
		{"above range", 1.5, []float64{1, 2, 3}, math.Inf(1)},
		{"median of three", 0.5, []float64{3, 1, 2}, 2},
		{"min", 0, []float64{3, 1, 2}, 1},
		{"max", 1, []float64{3, 1, 2}, 3},
		{"interpolated", 0.5, []float64{1, 2}, 1.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := quantile(tc.q, tc.values)
			if math.IsNaN(tc.want) {
				require.True(t, math.IsNaN(got))

				return
			}

			require.Equal(t, tc.want, got)
		})
	}
}

func TestAddRatioSample(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  float64
		offset float64
		want   bool
	}{
		{"zero limit admits nothing", 0, 0, false},
		{"positive limit admits below it", 0.4, 0.3, true},
		{"positive limit rejects at or above it", 0.4, 0.4, false},
		{"negative limit admits the complement", -0.6, 0.5, true},
		{"negative limit rejects below the complement", -0.6, 0.3, false},
		{"full positive limit admits everything", 1, 0.999, true},
		{"full negative limit admits everything", -1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, addRatioSample(tc.limit, tc.offset))
		})
	}
}

func TestClampRatio(t *testing.T) {
	require.Equal(t, 1.0, clampRatio(1.5))
	require.Equal(t, -1.0, clampRatio(-1.5))
	require.Equal(t, 0.3, clampRatio(0.3))
}
