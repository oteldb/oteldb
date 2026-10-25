package scarecrow

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/stretchr/testify/require"
)

func TestHistogramBufferSize(t *testing.T) {
	for _, tt := range []struct {
		name                           string
		current, samples, budget, want int
		fail                           bool
	}{
		{name: "unlimited", current: 2, samples: 3, want: 5},
		{name: "exact cap", current: 2, samples: 3, budget: 5, want: 5},
		{name: "cap exceeded", current: 2, samples: 3, budget: 4, fail: true},
		{name: "overflow", current: int(^uint(0)>>1) / 8, samples: 1, fail: true},
		{name: "no samples"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			size, err := histogramBufferSize(tt.current, tt.samples, newSampleBudget(tt.budget))
			if tt.fail {
				require.ErrorContains(t, err, "too many samples")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, size)
		})
	}
}

func TestHistogramBucketStorage(t *testing.T) {
	for _, tt := range []struct {
		name          string
		steps, stride int
		sparse        bool
	}{
		{name: "dense", steps: 256, stride: 1},
		{name: "bitmap", steps: 256, stride: 2},
		{name: "sparse", steps: 256, stride: 255, sparse: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var input Column
			input.Resize(0, tt.steps)
			for step := 0; step < tt.steps; step += tt.stride {
				input.Set(step, float64(step))
			}
			stored, err := copyHistogramBucket(t.Context(), &input, input.Count())
			require.NoError(t, err)
			require.Len(t, stored.values, input.Count())
			require.Equal(t, tt.sparse, stored.positions != nil)
			for i := range input.V {
				input.V[i] = -1
			}
			for step := range tt.steps {
				value, present := stored.valueAt(step)
				require.Equal(t, step%tt.stride == 0, present)
				if present {
					require.Equal(t, float64(step), value)
				}
			}
		})
	}
}

type histogramFixtureInput struct {
	schema                    *Schema
	columns                   []Column
	next                      int
	onSchema, onColumn, onEnd func()
}

func (*histogramFixtureInput) String() string       { return "HistogramFixture" }
func (*histogramFixtureInput) Children() []Operator { return nil }
func (*histogramFixtureInput) Close() error         { return nil }
func (i *histogramFixtureInput) Schema(context.Context) (*Schema, error) {
	if i.onSchema != nil {
		i.onSchema()
	}
	return i.schema, nil
}

func (i *histogramFixtureInput) Next(context.Context) (*Column, error) {
	if i.next == len(i.columns) {
		if i.onEnd != nil {
			i.onEnd()
		}
		return nil, nil
	}
	if i.onColumn != nil {
		i.onColumn()
	}
	column := &i.columns[i.next]
	i.next++
	return column, nil
}

func histogramFixture(t *testing.T, ec *EvalContext, inactive int) (*histogramQuantile, *histogramFixtureInput) {
	t.Helper()
	series := []labels.Labels{labels.FromStrings("__name__", "h_bucket", "le", "1"), labels.FromStrings("__name__", "h_bucket", "le", "+Inf")}
	for i := range inactive {
		series = append(series, labels.FromStrings("__name__", "h_bucket", "le", "1", "job", strconv.Itoa(i)))
	}
	input := &histogramFixtureInput{schema: NewSchema(series)}
	for i := range 2 {
		var column Column
		column.Resize(SeriesRef(i), ec.NumSteps())
		column.Set(ec.NumSteps()/2, float64(i+1))
		input.columns = append(input.columns, column)
	}
	call := &parser.Call{Args: parser.Expressions{&parser.NumberLiteral{Val: .5}, &parser.VectorSelector{Name: "h_bucket"}}}
	return newHistogramQuantile(input, newNumberLiteral(.5, ec), call, ec), input
}

func TestHistogramSparseSchemaSuperset(t *testing.T) {
	t.Parallel()
	const steps = 10_000
	ec := &EvalContext{Steps: make([]int64, steps), Budget: newSampleBudget(2)}
	operator, _ := histogramFixture(t, ec, 10_000)
	column, err := operator.Next(t.Context())
	require.NoError(t, err)
	require.NotNil(t, column)
	require.Equal(t, 1, column.Count())
	require.Equal(t, 1.0, column.V[steps/2])
	require.Len(t, operator.columns, 2)
	for _, stored := range operator.columns {
		require.Len(t, stored.values, 1)
		require.Len(t, stored.positions, 1)
		require.Nil(t, stored.valid)
	}
	column, err = operator.Next(t.Context())
	require.NoError(t, err)
	require.Nil(t, column)
}

func TestHistogramCancellation(t *testing.T) {
	for _, stage := range []string{"schema", "copy", "fold"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			operator, input := histogramFixture(t, &EvalContext{Steps: make([]int64, 100_000)}, 10)
			switch stage {
			case "schema":
				input.onSchema = cancel
			case "copy":
				input.onColumn = cancel
			case "fold":
				input.onEnd = cancel
			}
			column, err := operator.Next(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, column)
			if stage == "copy" {
				require.Empty(t, operator.columns)
			}
		})
	}
}
