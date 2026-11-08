package traceqlengine

import (
	"context"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/traceql"
)

type metricsPlanInput struct {
	schema             *scarecrow.Schema
	values             [][]float64
	next               int
	closed             bool
	schemaErr, nextErr error
	out                scarecrow.Column
	ec                 *scarecrow.EvalContext
}

func (*metricsPlanInput) String() string                 { return "TraceBuckets" }
func (*metricsPlanInput) Children() []scarecrow.Operator { return nil }
func (s *metricsPlanInput) Close() error                 { s.closed = true; return nil }
func (s *metricsPlanInput) Schema(context.Context) (*scarecrow.Schema, error) {
	return s.schema, s.schemaErr
}

func (s *metricsPlanInput) Next(ctx context.Context) (*scarecrow.Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.nextErr != nil {
		return nil, s.nextErr
	}
	if s.next == len(s.values) {
		return nil, nil
	}
	s.out.Resize(scarecrow.SeriesRef(s.next), len(s.values[s.next]))
	for i, value := range s.values[s.next] {
		if !math.IsNaN(value) {
			s.out.Set(i, value)
		}
	}
	if err := s.ec.ChargeSamples(s.out.Count()); err != nil {
		return nil, err
	}
	s.next++
	return &s.out, nil
}

func traceBucketInput(names []string, values ...[]float64) *metricsPlanInput {
	series := make([]labels.Labels, len(names))
	for i, name := range names {
		series[i] = labels.FromStrings("__name__", "rate()")
		if name != "" {
			series[i] = labels.FromStrings("__name__", "rate()", "svc", name)
		}
	}
	return &metricsPlanInput{schema: scarecrow.NewSchema(series), values: values}
}

type metricsPlanSource struct {
	inputs   []*metricsPlanInput
	requests []*traceql.MetricsAggregation
	failAt   int
	err      error
}

func (s *metricsPlanSource) Aggregation(_ context.Context, expr *traceql.MetricsAggregation, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	index := len(s.requests)
	s.requests = append(s.requests, expr)
	if s.err != nil && index == s.failAt {
		return nil, s.err
	}
	s.inputs[index].ec = ec
	return s.inputs[index], nil
}

type comparePlanSource struct {
	metricsPlanSource
	request *traceql.CompareOperation
}

func (s *comparePlanSource) Compare(_ context.Context, expr *traceql.CompareOperation, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	s.request = expr
	s.inputs[0].ec = ec
	return s.inputs[0], nil
}

func TestBuildMetricsCompare(t *testing.T) {
	t.Parallel()
	expr, err := traceql.Parse(`{} | compare({status=error}, 5)`)
	require.NoError(t, err)
	input := traceBucketInput([]string{"a"}, []float64{1})
	source := &comparePlanSource{metricsPlanSource: metricsPlanSource{inputs: []*metricsPlanInput{input}}}
	root, err := BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), source, scarecrow.NewEvalContext([]int64{0}, 10))
	require.NoError(t, err)
	require.Same(t, input, root)
	require.Same(t, expr, source.request)
	require.NoError(t, root.Close())
	require.True(t, input.closed)
}

func TestBuildMetricsSampleBudget(t *testing.T) {
	t.Parallel()
	expr, err := traceql.Parse(`{} | rate() | topk(1)`)
	require.NoError(t, err)
	input := traceBucketInput([]string{"a", "b"}, []float64{1, 2}, []float64{3, 4})
	source := &metricsPlanSource{inputs: []*metricsPlanInput{input}}
	_, err = BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), source, scarecrow.NewEvalContext([]int64{0, 1}, 3))
	require.ErrorContains(t, err, "too many samples")
	require.True(t, input.closed)
}

func TestBuildMetricsCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source := &metricsPlanSource{}
	_, err := BuildMetrics(ctx, nil, source, scarecrow.NewEvalContext([]int64{0}, 10))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, source.requests)
}

type fuzzMetricsSource struct{ inputs []*metricsPlanInput }

func (s *fuzzMetricsSource) Aggregation(_ context.Context, _ *traceql.MetricsAggregation, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	input := traceBucketInput([]string{"a"}, []float64{1})
	input.ec = ec
	s.inputs = append(s.inputs, input)
	return input, nil
}

func FuzzBuildMetrics(f *testing.F) {
	for _, query := range metricsPlanQueries {
		f.Add(query)
	}
	f.Add(`{} | compare({status=error})`)
	f.Add(`{} | rate() > 1 | topk(2) < 2 | bottomk(1)`)
	f.Fuzz(func(t *testing.T, query string) {
		expr, err := traceql.Parse(query)
		if err != nil {
			return
		}
		metric, ok := expr.(traceql.MetricsExpr)
		if !ok {
			return
		}
		source := &fuzzMetricsSource{}
		root, err := BuildMetrics(t.Context(), metric, source, scarecrow.NewEvalContext([]int64{0}, 100))
		if err == nil {
			for {
				col, err := root.Next(t.Context())
				if err != nil || col == nil {
					break
				}
			}
			require.NoError(t, root.Close())
		}
		for _, input := range source.inputs {
			require.True(t, input.closed)
		}
	})
}

var metricsPlanQueries = []string{
	`{} | rate() | topk(1)`,
	`{} | rate() | bottomk(1)`,
	`({} | rate()) * 100 > 200`,
	`2 / ({} | rate())`,
	`{} | avg_over_time(duration) > 500ms`,
	`({status=error} | rate() by(resource.service.name)) / ({} | rate())`,
	`({} | rate()) - ({} | rate() by(resource.service.name))`,
	`(({} | rate()) + ({} | rate())) * 2`,
}

func TestBuildMetrics(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		query  int
		inputs []*metricsPlanInput
		want   map[string][]float64
	}{
		{"topk", 0, []*metricsPlanInput{traceBucketInput([]string{"a", "b"}, []float64{1, 5}, []float64{3, 2})}, map[string][]float64{"a": {math.NaN(), 5}, "b": {3, math.NaN()}}},
		{"bottomk", 1, []*metricsPlanInput{traceBucketInput([]string{"a", "b"}, []float64{1, 5}, []float64{3, 2})}, map[string][]float64{"a": {1, math.NaN()}, "b": {math.NaN(), 2}}},
		{"scalar and filter", 2, []*metricsPlanInput{traceBucketInput([]string{"a"}, []float64{1, 3})}, map[string][]float64{"a": {math.NaN(), 300}}},
		{"left scalar and zero divisor", 3, []*metricsPlanInput{traceBucketInput([]string{"a"}, []float64{1, 0})}, map[string][]float64{"a": {2, math.NaN()}}},
		{"duration threshold in seconds", 4, []*metricsPlanInput{traceBucketInput([]string{"a"}, []float64{.25, .75})}, map[string][]float64{"a": {math.NaN(), .75}}},
		{"error ratio broadcast", 5, []*metricsPlanInput{traceBucketInput([]string{"a", "b"}, []float64{2, 4}, []float64{1, 0}), traceBucketInput([]string{""}, []float64{10, 0})}, map[string][]float64{"a": {.2, math.NaN()}, "b": {.1, math.NaN()}}},
		{"left broadcast preserves operand order", 6, []*metricsPlanInput{traceBucketInput([]string{""}, []float64{10, 6}), traceBucketInput([]string{"a", "b"}, []float64{2, 3}, []float64{5, 0})}, map[string][]float64{"a": {8, 3}, "b": {5, 6}}},
		{"nested arithmetic", 7, []*metricsPlanInput{traceBucketInput([]string{"a"}, []float64{1, 2}), traceBucketInput([]string{"a"}, []float64{3, 4})}, map[string][]float64{"a": {8, 12}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expr, err := traceql.Parse(metricsPlanQueries[tt.query])
			require.NoError(t, err)
			source := &metricsPlanSource{inputs: tt.inputs}
			root, err := BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), source, &scarecrow.EvalContext{Steps: []int64{0, 15_000}})
			require.NoError(t, err)
			schema, err := root.Schema(t.Context())
			require.NoError(t, err)
			got := map[string][]float64{}
			for {
				column, err := root.Next(t.Context())
				require.NoError(t, err)
				if column == nil {
					break
				}
				values := make([]float64, len(column.V))
				for i := range values {
					values[i] = math.NaN()
					if column.IsSet(i) {
						values[i] = column.V[i]
					}
				}
				got[schema.At(column.Ref).Get("svc")] = values
			}
			require.NoError(t, root.Close())
			require.Len(t, got, len(tt.want))
			for name, values := range tt.want {
				require.Len(t, got[name], len(values))
				for i, value := range values {
					if math.IsNaN(value) {
						require.True(t, math.IsNaN(got[name][i]))
					} else {
						require.Equal(t, value, got[name][i])
					}
				}
			}
			for _, input := range source.inputs {
				require.True(t, input.closed)
			}
			if tt.query == 5 {
				require.Equal(t, []traceql.Attribute{{Scope: traceql.ScopeResource, Name: "service.name"}}, source.requests[0].By)
				_, matchers := traceql.ExtractMatchers(source.requests[0].Spanset)
				require.Len(t, matchers, 1)
				require.Equal(t, traceql.SpanStatus, matchers[0].Attribute.Prop)
			}
		})
	}
}

func TestBuildMetricsFailureClosesSources(t *testing.T) {
	t.Parallel()
	failure := errors.New("source failure")
	for _, tt := range []struct {
		name, query                         string
		sourceError, schemaError, nextError bool
	}{
		{"second source", `({} | rate()) / ({} | rate())`, true, false, false},
		{"schema", `{} | rate()`, false, true, false},
		{"execution during ranking", `{} | rate() | topk(1)`, false, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := traceBucketInput([]string{"a"}, []float64{1})
			source := &metricsPlanSource{inputs: []*metricsPlanInput{input}}
			if tt.sourceError {
				source.failAt, source.err = 1, failure
			}
			if tt.schemaError {
				input.schemaErr = failure
			}
			if tt.nextError {
				input.nextErr = failure
			}
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			root, err := BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), source, &scarecrow.EvalContext{Steps: []int64{0}})
			require.ErrorIs(t, err, failure)
			require.Nil(t, root)
			require.True(t, input.closed)
		})
	}
}

func TestBuildMetricsUnsupportedCompare(t *testing.T) {
	t.Parallel()
	expr, err := traceql.Parse(`{} | compare({status=error})`)
	require.NoError(t, err)
	_, err = BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), &metricsPlanSource{}, &scarecrow.EvalContext{Steps: []int64{0}})
	var unsupported *UnsupportedError
	require.ErrorAs(t, err, &unsupported)
}
