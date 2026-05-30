package logqlmetric

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
)

func TestMergeBinOp(t *testing.T) {
	leftSteps := []Step{
		{
			Timestamp: 1,
			Samples: []Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
	}
	rightSteps := []Step{
		{
			Timestamp: 1,
			Samples: []Sample{
				{Data: 10, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 30, Set: mapLabels(map[string]string{"sample": "3"})},
				{Data: 40, Set: mapLabels(map[string]string{"sample": "4"})},
			},
		},
	}
	emptyStep := []Step{
		{Timestamp: 1},
	}

	tests := []struct {
		left, right []Step
		expr        *logql.BinOpExpr
		expect      []Sample
	}{
		{
			leftSteps, rightSteps,
			&logql.BinOpExpr{Op: logql.OpAnd},
			[]Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
		{
			leftSteps, rightSteps,
			&logql.BinOpExpr{Op: logql.OpOr},
			[]Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
				{Data: 40, Set: mapLabels(map[string]string{"sample": "4"})},
			},
		},
		{
			leftSteps, rightSteps,
			&logql.BinOpExpr{Op: logql.OpUnless},
			[]Sample{
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
			},
		},

		// Swap steps.
		{
			rightSteps, leftSteps, // notice the swap
			&logql.BinOpExpr{Op: logql.OpAnd},
			[]Sample{
				{Data: 10, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 30, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
		{
			rightSteps, leftSteps,
			&logql.BinOpExpr{Op: logql.OpOr},
			[]Sample{
				{Data: 10, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 30, Set: mapLabels(map[string]string{"sample": "3"})},
				{Data: 40, Set: mapLabels(map[string]string{"sample": "4"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
			},
		},
		{
			rightSteps, leftSteps,
			&logql.BinOpExpr{Op: logql.OpUnless},
			[]Sample{
				{Data: 40, Set: mapLabels(map[string]string{"sample": "4"})},
			},
		},

		// Special cases for empty steps.
		{
			leftSteps, emptyStep,
			&logql.BinOpExpr{Op: logql.OpAnd},
			nil,
		},
		{
			leftSteps, emptyStep,
			&logql.BinOpExpr{Op: logql.OpOr},
			[]Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
		{
			emptyStep, leftSteps,
			&logql.BinOpExpr{Op: logql.OpOr},
			[]Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
		{
			leftSteps, emptyStep,
			&logql.BinOpExpr{Op: logql.OpUnless},
			[]Sample{
				{Data: 1, Set: mapLabels(map[string]string{"sample": "1"})},
				{Data: 2, Set: mapLabels(map[string]string{"sample": "2"})},
				{Data: 3, Set: mapLabels(map[string]string{"sample": "3"})},
			},
		},
		{
			emptyStep, leftSteps,
			&logql.BinOpExpr{Op: logql.OpUnless},
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			left := iterators.Slice(tt.left)
			right := iterators.Slice(tt.right)

			iter, err := BinOp(left, right, tt.expr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, iter.Close())
			}()

			var result Step
			require.True(t, iter.Next(&result))
			require.Equal(t, tt.left[0].Timestamp, result.Timestamp)
			require.Equal(t, tt.expect, result.Samples)

			require.False(t, iter.Next(&result))
			require.NoError(t, iter.Err())
		})
	}
}
