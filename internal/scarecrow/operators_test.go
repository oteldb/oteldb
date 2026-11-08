package scarecrow_test

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type metricInput struct {
	schema *scarecrow.Schema
	values [][]float64
	next   int
	out    scarecrow.Column
	closed bool
}

func (*metricInput) String() string                                      { return "MetricInput" }
func (*metricInput) Children() []scarecrow.Operator                      { return nil }
func (s *metricInput) Close() error                                      { s.closed = true; return nil }
func (s *metricInput) Schema(context.Context) (*scarecrow.Schema, error) { return s.schema, nil }
func (s *metricInput) Next(context.Context) (*scarecrow.Column, error) {
	if s.next == len(s.values) {
		return nil, nil
	}
	s.out.Resize(scarecrow.SeriesRef(s.next), len(s.values[s.next]))
	for i, value := range s.values[s.next] {
		s.out.Set(i, value)
	}
	s.next++
	return &s.out, nil
}

func metricSource(names []string, values ...[]float64) *metricInput {
	series := make([]labels.Labels, len(names))
	for i, name := range names {
		series[i] = labels.FromStrings("__name__", "calls")
		if name != "" {
			series[i] = labels.FromStrings("__name__", "calls", "svc", name)
		}
	}
	return &metricInput{schema: scarecrow.NewSchema(series), values: values}
}

func eventOptions() scarecrow.BinaryOptions {
	return scarecrow.BinaryOptions{
		NaNOnZeroDivision: true, NaNIsAbsent: true,
		PreserveMetricNames: true, BroadcastUnlabelled: true,
	}
}

func readMetricColumns(t *testing.T, root scarecrow.Operator) (map[string][]float64, *scarecrow.Schema) {
	t.Helper()
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	require.NoError(t, scarecrow.Prepare(t.Context(), root))
	schema, err := root.Schema(t.Context())
	require.NoError(t, err)
	got := map[string][]float64{}
	for {
		col, err := root.Next(t.Context())
		require.NoError(t, err)
		if col == nil {
			break
		}
		values := make([]float64, len(col.V))
		for i := range values {
			values[i] = math.NaN()
			if col.IsSet(i) {
				values[i] = col.V[i]
			}
		}
		got[schema.At(col.Ref).Get("svc")] = values
	}
	return got, schema
}

func equalMetricValues(t *testing.T, want, got []float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, value := range want {
		if math.IsNaN(value) {
			require.True(t, math.IsNaN(got[i]), "step %d: %v", i, got)
		} else {
			require.Equal(t, value, got[i], "step %d", i)
		}
	}
}

func TestSharedScalarOperators(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		op   scarecrow.BinaryOp
		left bool
		want []float64
	}{
		{"add", scarecrow.BinaryAdd, false, []float64{4, 2, 0}},
		{"subtract", scarecrow.BinarySub, false, []float64{0, -2, -4}},
		{"left subtract", scarecrow.BinarySub, true, []float64{0, 2, 4}},
		{"multiply", scarecrow.BinaryMul, false, []float64{4, 0, -4}},
		{"divide", scarecrow.BinaryDiv, false, []float64{1, 0, -1}},
		{"left divide", scarecrow.BinaryDiv, true, []float64{1, math.NaN(), -1}},
		{"equal", scarecrow.BinaryEqual, false, []float64{2, math.NaN(), math.NaN()}},
		{"not equal", scarecrow.BinaryNotEqual, false, []float64{math.NaN(), 0, -2}},
		{"greater", scarecrow.BinaryGreater, false, []float64{math.NaN(), math.NaN(), math.NaN()}},
		{"greater equal", scarecrow.BinaryGreaterEqual, false, []float64{2, math.NaN(), math.NaN()}},
		{"less", scarecrow.BinaryLess, false, []float64{math.NaN(), 0, -2}},
		{"less equal", scarecrow.BinaryLessEqual, false, []float64{2, 0, -2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root, err := scarecrow.NewScalarBinary(metricSource([]string{"a"}, []float64{2, 0, -2}), 2, tt.op, tt.left, eventOptions(), &scarecrow.EvalContext{Steps: []int64{0, 1, 2}})
			require.NoError(t, err)
			got, schema := readMetricColumns(t, root)
			require.Equal(t, "calls", schema.At(0).Get("__name__"))
			if _, exists := got["a"]; !exists {
				for _, value := range tt.want {
					require.True(t, math.IsNaN(value))
				}
			} else {
				equalMetricValues(t, tt.want, got["a"])
			}
		})
	}
}

func TestSharedVectorBroadcast(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		left, right []string
		lv, rv      [][]float64
		want        map[string][]float64
	}{
		{"matched intersection", []string{"a", "b"}, []string{"a", "c"}, [][]float64{{8, 4}, {6, 3}}, [][]float64{{2, 0}, {3, 2}}, map[string][]float64{"a": {4, math.NaN()}}},
		{"ungrouped right", []string{"a", "b"}, []string{""}, [][]float64{{8, 4}, {6, 3}}, [][]float64{{2, 0}}, map[string][]float64{"a": {4, math.NaN()}, "b": {3, math.NaN()}}},
		{"ungrouped left", []string{""}, []string{"a", "b"}, [][]float64{{12, 9}}, [][]float64{{4, 3}, {3, 0}}, map[string][]float64{"a": {3, 3}, "b": {4, math.NaN()}}},
		{"exact before fallback", []string{"a", ""}, []string{"a", "b"}, [][]float64{{100, 40}, {5, 6}}, [][]float64{{10, 4}, {20, 3}}, map[string][]float64{"a": {10, 10}, "b": {.25, 2}}},
		{"both ungrouped", []string{""}, []string{""}, [][]float64{{8, 4}}, [][]float64{{2, 0}}, map[string][]float64{"": {4, math.NaN()}}},
		{"empty right", []string{"a"}, nil, [][]float64{{8, 4}}, nil, map[string][]float64{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root, err := scarecrow.NewVectorBinary(metricSource(tt.left, tt.lv...), metricSource(tt.right, tt.rv...), scarecrow.BinaryDiv, eventOptions(), &scarecrow.EvalContext{Steps: []int64{0, 1}})
			require.NoError(t, err)
			got, schema := readMetricColumns(t, root)
			require.Len(t, got, len(tt.want))
			for name, values := range tt.want {
				equalMetricValues(t, values, got[name])
			}
			for _, ls := range schema.Series {
				require.Equal(t, "(calls / calls)", ls.Get("__name__"))
			}
		})
	}
}

func TestSharedNaNPolicy(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		op         scarecrow.BinaryOp
		value      float64
		options    scarecrow.BinaryOptions
		wantValues []float64
		wantValid  []bool
	}{
		{"default division", scarecrow.BinaryDiv, 0, scarecrow.BinaryOptions{}, []float64{math.Inf(1), math.NaN()}, []bool{true, true}},
		{"NaN instead of infinity", scarecrow.BinaryDiv, 0, scarecrow.BinaryOptions{NaNOnZeroDivision: true}, []float64{math.NaN(), math.NaN()}, []bool{true, true}},
		{"division gaps", scarecrow.BinaryDiv, 0, eventOptions(), nil, nil},
		{"NaN not equal", scarecrow.BinaryNotEqual, 0, eventOptions(), []float64{1, 0}, []bool{true, false}},
		{"NaN threshold", scarecrow.BinaryNotEqual, math.NaN(), eventOptions(), nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root, err := scarecrow.NewScalarBinary(metricSource([]string{"a"}, []float64{1, math.NaN()}), tt.value, tt.op, false, tt.options, &scarecrow.EvalContext{Steps: []int64{0, 1}})
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			require.NoError(t, scarecrow.Prepare(t.Context(), root))
			col, err := root.Next(t.Context())
			require.NoError(t, err)
			if tt.wantValues == nil {
				require.Nil(t, col)
				return
			}
			require.NotNil(t, col)
			for i, valid := range tt.wantValid {
				require.Equal(t, valid, col.IsSet(i))
				if valid {
					equalMetricValues(t, tt.wantValues[i:i+1], col.V[i:i+1])
				}
			}
		})
	}
}

func TestSharedOperatorValidation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		build func(scarecrow.Operator, *scarecrow.EvalContext) (scarecrow.Operator, error)
	}{
		{"unknown scalar operation", func(input scarecrow.Operator, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewScalarBinary(input, 1, 0, false, eventOptions(), ec)
		}},
		{"unknown vector operation", func(input scarecrow.Operator, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewVectorBinary(input, input, 0, eventOptions(), ec)
		}},
		{"vector comparison", func(input scarecrow.Operator, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewVectorBinary(input, input, scarecrow.BinaryEqual, eventOptions(), ec)
		}},
		{"missing context", func(input scarecrow.Operator, _ *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewTopK(input, 1, nil)
		}},
		{"zero topk", func(input scarecrow.Operator, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewTopK(input, 0, ec)
		}},
		{"negative bottomk", func(input scarecrow.Operator, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
			return scarecrow.NewBottomK(input, -1, ec)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := metricSource([]string{"a"}, []float64{1})
			root, err := tt.build(input, scarecrow.NewEvalContext([]int64{0}, 10))
			require.Error(t, err)
			require.Nil(t, root)
			require.False(t, input.closed)
			require.Zero(t, input.next)
			require.NoError(t, input.Close())
		})
	}
}

func TestSharedRankOperators(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		bottom bool
		limit  int
		want   map[string][]float64
	}{
		{"top", false, 1, map[string][]float64{"a": {math.NaN(), 5}, "b": {3, math.NaN()}}},
		{"bottom", true, 1, map[string][]float64{"a": {1, math.NaN()}, "b": {math.NaN(), 2}}},
		{"large limit", false, math.MaxInt, map[string][]float64{"a": {1, 5}, "b": {3, 2}, "c": {2, 4}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := metricSource([]string{"a", "b", "c"}, []float64{1, 5}, []float64{3, 2}, []float64{2, 4})
			build := scarecrow.NewTopK
			if tt.bottom {
				build = scarecrow.NewBottomK
			}
			root, err := build(input, tt.limit, &scarecrow.EvalContext{Steps: []int64{0, 1}})
			require.NoError(t, err)
			got, _ := readMetricColumns(t, root)
			require.Len(t, got, len(tt.want))
			for name, values := range tt.want {
				equalMetricValues(t, values, got[name])
			}
		})
	}
}
