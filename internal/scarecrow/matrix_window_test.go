package scarecrow

import (
	"math"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/value"
	"github.com/stretchr/testify/require"
)

func TestMatrixFoldWindow(t *testing.T) {
	t.Parallel()
	stale := math.Float64frombits(value.StaleNaN)
	for _, tt := range []struct {
		name    string
		values  []float64
		weights []float64
		keep    []int
		borrow  bool
	}{
		{"plain", []float64{1, 2, 3, 4}, nil, []int{1, 2}, true},
		{"weighted", []float64{1, 2, 3, 4}, []float64{1, 3, 5, 7}, []int{1, 2}, true},
		{"stale first", []float64{1, stale, 3, 4}, nil, []int{2}, false},
		{"stale last", []float64{1, 2, stale, 4}, []float64{1, 3, 5, 7}, []int{1}, false},
		{"all stale", []float64{1, stale, stale, 4}, nil, nil, false},
		{"ordinary NaN", []float64{1, math.NaN(), 3, 4}, nil, []int{1, 2}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			samples := Samples{T: []int64{10, 20, 30, 40}, V: tt.values, Weights: tt.weights}
			original := slices.Clone(samples.V)
			var fold matrixFold
			window := fold.buildWindow(&samples, 1, 3, 15, 35, 20, 50)
			require.Equal(t, int64(15), window.RangeStart)
			require.Equal(t, int64(35), window.RangeEnd)
			require.Equal(t, int64(20), window.RangeMs)
			require.Equal(t, int64(50), window.EvalMs)
			require.Len(t, window.T, len(tt.keep))
			require.Len(t, window.V, len(tt.keep))
			for i, index := range tt.keep {
				require.Equal(t, samples.T[index], window.T[i])
				require.Equal(t, math.Float64bits(samples.V[index]), math.Float64bits(window.V[i]))
				if tt.weights != nil {
					require.Equal(t, tt.weights[index], window.W[i])
				}
			}
			if tt.weights == nil {
				require.Nil(t, window.W)
			}
			if tt.borrow {
				require.Same(t, &samples.V[1], &window.V[0])
				require.Same(t, &samples.T[1], &window.T[0])
				if tt.weights != nil {
					require.Same(t, &samples.Weights[1], &window.W[0])
				}
			}
			for i := range original {
				require.Equal(t, math.Float64bits(original[i]), math.Float64bits(samples.V[i]))
			}
			plain := Samples{T: []int64{100}, V: []float64{9}}
			next := fold.buildWindow(&plain, 0, 1, 90, 110, 20, 110)
			require.Same(t, window, next)
			require.Nil(t, next.W, "a previous weighted window must not leak weights")
			require.Equal(t, []float64{9}, next.V)
		})
	}
}

func FuzzMatrixWindowInputOwnership(f *testing.F) {
	for _, seed := range [][]byte{{}, {0}, {1, 2, 3, 4}, {255, 1, 255, 2}} {
		f.Add(seed, false)
		f.Add(seed, true)
	}
	f.Fuzz(func(t *testing.T, raw []byte, weighted bool) {
		if len(raw) > 1024 {
			return
		}
		samples := Samples{T: make([]int64, len(raw)), V: make([]float64, len(raw))}
		if weighted {
			samples.Weights = make([]float64, len(raw))
		}
		var expectedT []int64
		var expectedV, expectedW []float64
		for i, b := range raw {
			samples.T[i], samples.V[i] = int64(i), float64(b)
			if weighted {
				samples.Weights[i] = float64(b) + 1
			}
			if b == 255 {
				samples.V[i] = math.Float64frombits(value.StaleNaN)
				continue
			}
			expectedT = append(expectedT, int64(i))
			expectedV = append(expectedV, float64(b))
			if weighted {
				expectedW = append(expectedW, float64(b)+1)
			}
		}
		original := slices.Clone(samples.V)
		var fold matrixFold
		window := fold.buildWindow(&samples, 0, len(raw), -1, int64(len(raw)), int64(len(raw))+1, 0)
		require.Equal(t, len(expectedT), len(window.T))
		require.Equal(t, len(expectedV), len(window.V))
		for i := range expectedT {
			require.Equal(t, expectedT[i], window.T[i])
			require.Equal(t, expectedV[i], window.V[i])
			if weighted {
				require.Equal(t, expectedW[i], window.W[i])
			}
		}
		for name, fn := range rangeFuncs {
			fn(window, 0.5)
			for i := range original {
				require.Equal(t, math.Float64bits(original[i]), math.Float64bits(samples.V[i]), name)
			}
		}
	})
}
