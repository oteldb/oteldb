package logql

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReduceBinOp(t *testing.T) {
	tests := []struct {
		input  string
		want   float64
		reduce bool
	}{
		{`1 + 1`, 2, true},
		{`2 - 1`, 1, true},
		{`2 * 2`, 4, true},
		{`2 / 2`, 1, true},
		{`2 / 0`, math.NaN(), true},
		{`5 % 2`, 1, true},
		{`5 % 0`, math.NaN(), true},
		{`2 ^ 3`, 8, true},

		{`2 + 2 * 2`, 6, true},
		{`(2 + 2) * 2`, 8, true},
		{`2 * 2 ^ 2`, 8, true},
		{`(2 * 2) ^ 2`, 16, true},

		{`2 == 2`, 1., true},
		{`2 == 3`, 0., true},
		{`2 != 2`, 0., true},
		{`2 != 3`, 1., true},

		{`3 > 2`, 1., true},
		{`3 > 3`, 0., true},
		{`3 > 4`, 0., true},
		{`3 >= 2`, 1., true},
		{`3 >= 3`, 1., true},
		{`3 >= 4`, 0., true},

		{`2 < 3`, 1., true},
		{`2 < 2`, 0., true},
		{`2 < 1`, 0., true},
		{`2 <= 3`, 1., true},
		{`2 <= 2`, 1., true},
		{`2 <= 1`, 0., true},

		{`4 % 3 * 2`, 2, true},
		{`2 * 3 % 2`, 0, true},

		{`2 + 3*4`, 14., true},
		{`2*3 + 4`, 10., true},
		{`2 + 3*4 + 5`, 19., true},
		{`2 + 3^2`, 11., true},
		{`2 * 3^2`, 18., true},
		{`2^3 * 2`, 16., true},
		{`2 ^ 3^2`, 512., true},

		{`0-1+2*3/4%5^6`, 0.5, true},

		{`2*(3+4)`, 14., true},
		{`(2+3)*4`, 20., true},
		{`(3+2)*(2+3)`, 25., true},
		{`((3+2)*(2+3))`, 25., true},
		{`(2^3)^2`, 64., true},

		{`vector(0) + vector(0)`, 0, false},
		{`0 + vector(0)`, 0, false},
		{`vector(0) + 0`, 0, false},
		{`(vector(0) + vector(0)) * 2`, 0, false},
		{`2 * (vector(0) + vector(0))`, 0, false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			expr, err := Parse(tt.input, ParseOptions{AllowDots: true})
			require.NoError(t, err, "invalid input")

			expr = UnparenExpr(expr)
			require.IsType(t, &BinOpExpr{}, expr, "wrong expression type")
			binOp := expr.(*BinOpExpr)

			result, err := ReduceBinOp(binOp)
			require.NoError(t, err)

			if !tt.reduce {
				require.Nil(t, result)
				return
			}
			require.NotNil(t, result)

			if math.IsNaN(tt.want) {
				require.True(t, math.IsNaN(result.Value))
			} else {
				require.Equal(t, tt.want, result.Value)
			}
		})
	}
}
