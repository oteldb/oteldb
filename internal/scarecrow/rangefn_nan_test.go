package scarecrow

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMadOverTimeNaN(t *testing.T) {
	for _, tt := range []struct {
		name   string
		values []float64
		want   float64
		ok     bool
	}{
		{name: "empty"},
		{name: "finite", values: []float64{1, 2, 3, 4, 5}, want: 1, ok: true},
		{name: "single", values: []float64{3}, want: 0, ok: true},
		{name: "only_nan", values: []float64{math.NaN()}, want: math.NaN(), ok: true},
		{name: "nan_first", values: []float64{math.NaN(), 1, 2, 3, 4}, want: math.NaN(), ok: true},
		{name: "nan_middle", values: []float64{1, 2, math.NaN(), 3, 4}, want: math.NaN(), ok: true},
		{name: "nan_last", values: []float64{1, 2, 3, 4, math.NaN()}, want: math.NaN(), ok: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := append([]float64(nil), tt.values...)
			got, ok := madOverTime(&window{T: make([]int64, len(values)), V: values})
			require.Equal(t, tt.ok, ok)
			if math.IsNaN(tt.want) {
				require.True(t, math.IsNaN(got))
			} else {
				require.Equal(t, tt.want, got)
			}
			for i, v := range values {
				if math.IsNaN(tt.values[i]) {
					require.True(t, math.IsNaN(v))
				} else {
					require.Equal(t, tt.values[i], v)
				}
			}
		})
	}
}
