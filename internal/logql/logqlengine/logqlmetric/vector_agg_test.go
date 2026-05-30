package logqlmetric

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
)

func ptrTo[T any](v T) *T {
	return &v
}

func TestSortVectorAggregation(t *testing.T) {
	steps := []Step{
		{
			Timestamp: 1,
			Samples: []Sample{
				{Data: 3, Set: emptyLabels()},
				{Data: 1, Set: emptyLabels()},
				{Data: 4, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
			},
		},
	}

	tests := []struct {
		expr   *logql.VectorAggregationExpr
		expect []Sample
	}{
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpBottomk, Parameter: ptrTo(2)},
			[]Sample{
				{Data: 1, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
			},
		},
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpBottomk, Parameter: ptrTo(3)},
			[]Sample{
				{Data: 1, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
				{Data: 3, Set: emptyLabels()},
			},
		},
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpTopk, Parameter: ptrTo(2)},
			[]Sample{
				{Data: 4, Set: emptyLabels()},
				{Data: 3, Set: emptyLabels()},
			},
		},
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpTopk, Parameter: ptrTo(3)},
			[]Sample{
				{Data: 4, Set: emptyLabels()},
				{Data: 3, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
			},
		},
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpSort},
			[]Sample{
				{Data: 1, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
				{Data: 3, Set: emptyLabels()},
				{Data: 4, Set: emptyLabels()},
			},
		},
		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpSortDesc},
			[]Sample{
				{Data: 4, Set: emptyLabels()},
				{Data: 3, Set: emptyLabels()},
				{Data: 2, Set: emptyLabels()},
				{Data: 1, Set: emptyLabels()},
			},
		},

		{
			&logql.VectorAggregationExpr{Op: logql.VectorOpBottomk, Parameter: ptrTo(0)},
			nil,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			input := iterators.Slice(steps)

			iter, err := VectorAggregation(input, tt.expr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, iter.Close())
			}()

			var result Step
			require.True(t, iter.Next(&result))
			require.Equal(t, steps[0].Timestamp, result.Timestamp)
			require.Equal(t, tt.expect, result.Samples)

			require.False(t, iter.Next(&result))
			require.NoError(t, iter.Err())
		})
	}
}
