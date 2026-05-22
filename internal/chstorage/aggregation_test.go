package chstorage

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestPromQuerier_AggregateSampledPoints(t *testing.T) {
	ctx := context.Background()
	p := &promQuerier{
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	sampler, ok := canUseSampledPoints(15*time.Second, "sum")
	require.True(t, ok)

	h1 := [16]byte{1}
	h2 := [16]byte{2}
	
	// Use label matches that will actually match 'by (job)'
	lb1 := labels.FromStrings("job", "test", "instance", "1")
	lb2 := labels.FromStrings("job", "test", "instance", "2")
	commonLabels := labels.FromStrings("job", "test")

	step := 15 * time.Second
	stepMs := step.Milliseconds()
	minTS := int64(1000 * 1000)
	
	set := map[[16]byte]*series[pointData]{
		h1: {
			labels: lb1,
			ts:     []int64{minTS, minTS + stepMs, minTS + 2*stepMs},
			data:   pointData{values: []float64{1, 2, 3}},
		},
		h2: {
			labels: lb2,
			ts:     []int64{minTS, minTS + stepMs, minTS + 2*stepMs},
			data:   pointData{values: []float64{10, 20, 30}},
		},
	}

	t.Run("Slice", func(t *testing.T) {
		res := p.aggregateSampledPoints(ctx, set, true, []string{"job"}, sampler, step)
		require.Len(t, res, 1)
		
		s := res[0]
		assert.Equal(t, commonLabels.String(), s.labels.String())
		assert.Equal(t, []int64{minTS, minTS + stepMs, minTS + 2*stepMs}, s.ts)
		assert.Equal(t, []float64{11, 22, 33}, s.data.values)
	})

	t.Run("MapFallback", func(t *testing.T) {
		res := p.aggregateSampledPointsMap(ctx, set, true, []string{"job"}, sampler)
		require.Len(t, res, 1)
		
		s := res[0]
		assert.Equal(t, commonLabels.String(), s.labels.String())
		assert.Equal(t, []int64{minTS, minTS + stepMs, minTS + 2*stepMs}, s.ts)
		assert.Equal(t, []float64{11, 22, 33}, s.data.values)
	})

	t.Run("Empty", func(t *testing.T) {
		res := p.aggregateSampledPoints(ctx, nil, true, []string{"job"}, sampler, step)
		assert.Nil(t, res)
	})

	t.Run("SingleSeries", func(t *testing.T) {
		singleSet := map[[16]byte]*series[pointData]{h1: set[h1]}
		res := p.aggregateSampledPoints(ctx, singleSet, true, []string{"job", "instance"}, sampler, step)
		require.Len(t, res, 1)
		assert.Equal(t, lb1.String(), res[0].labels.String())
		assert.Equal(t, set[h1].ts, res[0].ts)
		assert.Equal(t, set[h1].data.values, res[0].data.values)
	})
	
	t.Run("Gaps", func(t *testing.T) {
		gapSet := map[[16]byte]*series[pointData]{
			h1: {
				labels: lb1,
				ts:     []int64{minTS, minTS + 2*stepMs},
				data:   pointData{values: []float64{1, 3}},
			},
			h2: {
				labels: lb2,
				ts:     []int64{minTS + stepMs},
				data:   pointData{values: []float64{20}},
			},
		}
		res := p.aggregateSampledPoints(ctx, gapSet, true, []string{"job"}, sampler, step)
		require.Len(t, res, 1)
		assert.Equal(t, []int64{minTS, minTS + stepMs, minTS + 2*stepMs}, res[0].ts)
		assert.Equal(t, []float64{1, 20, 3}, res[0].data.values)
	})
}

func TestPromQuerier_AggregateSampledPoints_LargeRange(t *testing.T) {
	ctx := context.Background()
	p := &promQuerier{
		tracer: noop.NewTracerProvider().Tracer("test"),
	}
	sampler, _ := canUseSampledPoints(15*time.Second, "sum")
	step := 15 * time.Second
	
	h1 := [16]byte{1}
	// 200,000 steps range.
	minTS := int64(0)
	maxTS := int64(200_000 * 15 * 1000)
	
	set := map[[16]byte]*series[pointData]{
		h1: {
			labels: labels.FromStrings("job", "test"),
			ts:     []int64{minTS, maxTS},
			data:   pointData{values: []float64{1, 2}},
		},
	}
	
	// Should fallback to map to avoid huge slice allocation.
	res := p.aggregateSampledPoints(ctx, set, true, []string{"job"}, sampler, step)
	require.Len(t, res, 1)
	assert.Equal(t, []int64{minTS, maxTS}, res[0].ts)
	assert.Equal(t, []float64{1, 2}, res[0].data.values)
}

func TestPromQuerier_AggregateSampledPoints_Misaligned(t *testing.T) {
	ctx := context.Background()
	p := &promQuerier{
		tracer: noop.NewTracerProvider().Tracer("test"),
	}
	sampler, _ := canUseSampledPoints(15*time.Second, "sum")
	step := 15 * time.Second
	stepMs := step.Milliseconds()
	
	h1 := [16]byte{1}
	minTS := int64(1000 * 1000)
	
	set := map[[16]byte]*series[pointData]{
		h1: {
			labels: labels.FromStrings("job", "test"),
			// Second point is misaligned (not a multiple of 15s).
			ts:     []int64{minTS, minTS + stepMs + 1},
			data:   pointData{values: []float64{1, 2}},
		},
	}
	
	// Should fallback to map.
	res := p.aggregateSampledPoints(ctx, set, true, []string{"job"}, sampler, step)
	require.Len(t, res, 1)
	assert.Equal(t, []int64{minTS, minTS + stepMs + 1}, res[0].ts)
	assert.Equal(t, []float64{1, 2}, res[0].data.values)
}
