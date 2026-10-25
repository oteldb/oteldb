package scarecrow

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type projectedCountScanner struct {
	Scanner
	series []GridAggregate
	calls  int
}

func (s *projectedCountScanner) AggregateGrid(context.Context, WindowGrid, []*labels.Matcher) ([]GridAggregate, error) {
	s.calls++
	return s.series, nil
}

func (*projectedCountScanner) CountSeriesBy(context.Context, int64, int64, string, []*labels.Matcher) (map[string]uint64, error) {
	panic("grid query used per-window count")
}

func projectionFixture() (*projectedCountScanner, *EvalContext) {
	var series []GridAggregate
	for _, r := range []struct {
		name  string
		steps []int
	}{
		{"idle", []int{1, 2, 5}}, {"busy", []int{2, 3, 5}}, {"idle", []int{1, 4, 5}}, {"busy", []int{3, 4, 5}}, {"", []int{4}}, {"", []int{1}}, {"ghost", nil},
	} {
		set := labels.EmptyLabels()
		if r.name != "" {
			set = labels.FromStrings("mode", r.name)
		}
		windows := make([]Aggregate, 6)
		for _, step := range r.steps {
			windows[step] = Aggregate{Rows: 1, Count: 99}
		}
		series = append(series, GridAggregate{Labels: set, Windows: windows})
	}
	ec := &EvalContext{Steps: []int64{0, 1000, 2000, 3000, 4000, 5000}, Interval: time.Second, LookbackDelta: time.Second}
	return &projectedCountScanner{series: series}, ec
}

func TestCountGridProjection(t *testing.T) {
	t.Parallel()
	scanner, ec := projectionFixture()
	scanner.series[5].Labels = labels.FromStrings("mode", "")
	op := newCountSeriesBy(scanner, scanner, nil, "", "mode", 0, nil, ec)
	schema, err := op.Schema(t.Context())
	require.NoError(t, err)
	require.Equal(t, []labels.Labels{labels.EmptyLabels(), labels.FromStrings("mode", "busy"), labels.FromStrings("mode", "idle")}, schema.Series)
	expected := [][]float64{{0, 1, 0, 0, 1, 0}, {0, 0, 1, 2, 1, 2}, {0, 2, 1, 0, 1, 2}}
	for i, want := range expected {
		col, err := op.Next(t.Context())
		require.NoError(t, err)
		require.NotNil(t, col)
		assert.Equal(t, SeriesRef(i), col.Ref)
		for step, n := range want {
			assert.Equal(t, n, col.V[step])
			assert.Equal(t, n > 0, col.IsSet(step))
		}
	}
	col, err := op.Next(t.Context())
	require.NoError(t, err)
	assert.Nil(t, col)
	assert.Equal(t, 1, scanner.calls)
}

func TestCountGridProjectionBudget(t *testing.T) {
	t.Parallel()
	scanner, ec := projectionFixture()
	ec.Budget = newSampleBudget(18)
	op := newCountSeriesBy(scanner, scanner, nil, "", "mode", 0, nil, ec)
	_, err := op.Schema(t.Context())
	require.NoError(t, err, "budget counts the three active groups, not input series")
	assert.Equal(t, int64(18), ec.Budget.used.Load())
	scanner, ec = projectionFixture()
	ec.Budget = newSampleBudget(17)
	op = newCountSeriesBy(scanner, scanner, nil, "", "mode", 0, nil, ec)
	_, err = op.Schema(t.Context())
	require.ErrorAs(t, err, new(promql.ErrTooManySamples))
	assert.Empty(t, op.values, "reject before allocating the output grid")
}

func BenchmarkCountGridProjection(b *testing.B) {
	const steps = 1441
	series := make([]GridAggregate, 128)
	for i := range series {
		windows := make([]Aggregate, steps)
		for step := range windows {
			if step >= 720 {
				windows[step] = Aggregate{Rows: 1}
			}
		}
		series[i] = GridAggregate{Labels: labels.FromStrings("cpu", strconv.Itoa(i%16)), Windows: windows}
	}
	timestamps := make([]int64, steps)
	for i := range timestamps {
		timestamps[i] = int64(i) * 60000
	}
	scanner := &projectedCountScanner{series: series}
	ec := &EvalContext{Steps: timestamps, Interval: time.Minute, LookbackDelta: 5 * time.Minute}
	b.ReportAllocs()
	for b.Loop() {
		op := newCountSeriesBy(scanner, scanner, nil, "", "cpu", 0, nil, ec)
		if _, err := op.Schema(b.Context()); err != nil {
			b.Fatal(err)
		}
		for {
			col, err := op.Next(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			if col == nil {
				break
			}
		}
	}
}

func TestCountWindowCalls(t *testing.T) {
	t.Parallel()
	at := int64(300)
	for _, tc := range []struct {
		name string
		refs []int64
		at   *int64
		want int
	}{
		{"empty", nil, nil, 0}, {"range", []int64{1, 2, 3}, nil, 3}, {"pinned", []int64{300, 300, 300}, &at, 1}, {"empty pinned", nil, &at, 0},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); assert.Equal(t, tc.want, countWindowCalls(tc.refs, tc.at)) })
	}
}

func TestCountGridProjectionEmpty(t *testing.T) {
	t.Parallel()
	scanner, ec := projectionFixture()
	scanner.series = nil
	op := newCountSeriesBy(scanner, scanner, nil, "", "mode", 0, nil, ec)
	schema, err := op.Schema(t.Context())
	require.NoError(t, err)
	assert.Zero(t, schema.Len())
	col, err := op.Next(t.Context())
	require.NoError(t, err)
	assert.Nil(t, col)
	assert.Equal(t, 1, scanner.calls)
}

func TestCountGridProjectionInvalidWindows(t *testing.T) {
	t.Parallel()
	scanner, ec := projectionFixture()
	scanner.series[0].Windows = nil
	op := newCountSeriesBy(scanner, scanner, nil, "", "mode", 0, nil, ec)
	_, err := op.Schema(t.Context())
	require.ErrorContains(t, err, "got 0 windows, want 6")
}
