package traceqlengine

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricsGrid(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_010, 0).UTC()
	for _, tt := range []struct {
		name        string
		p           MetricsEvalParams
		max, points int
		bad         bool
	}{
		{"range alignment", MetricsEvalParams{Start: base, End: base.Add(time.Minute), Step: 15 * time.Second}, 100, 5, false},
		{"unaligned range", MetricsEvalParams{Start: base.Add(time.Millisecond), End: base.Add(31 * time.Second), Step: 15 * time.Second}, 100, 4, false},
		{"instant", MetricsEvalParams{Start: base, End: base.Add(time.Minute), Instant: true}, 100, 1, false},
		{"zero width", MetricsEvalParams{Start: base, End: base, Step: time.Second}, 100, 0, true},
		{"negative timestamp", MetricsEvalParams{Start: time.Unix(-1, 0), End: base, Step: time.Second}, 100, 0, true},
		{"submillisecond", MetricsEvalParams{Start: base, End: base.Add(time.Second), Step: time.Microsecond}, 100, 0, true},
		{"fractional millisecond", MetricsEvalParams{Start: base, End: base.Add(time.Second), Step: 1500 * time.Microsecond}, 100, 0, true},
		{"zero step", MetricsEvalParams{Start: base, End: base.Add(time.Second)}, 100, 0, true},
		{"too many steps", MetricsEvalParams{Start: base, End: base.Add(time.Minute), Step: time.Millisecond}, 100, 0, true},
		{"alignment overflow", MetricsEvalParams{Start: time.Unix(0, math.MaxInt64-20), End: time.Unix(0, math.MaxInt64-1), Step: time.Second}, 100, 0, true},
		{"timestamp wraps", MetricsEvalParams{Start: base, End: time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC), Step: time.Hour}, 100, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			g, err := newMetricsGrid(tt.p, tt.max)
			if tt.bad {
				var params *MetricsParamsError
				require.ErrorAs(t, err, &params)
				return
			}
			require.NoError(t, err)
			require.Len(t, g.steps, tt.points)
			if tt.p.Instant {
				_, ok := g.bucket(g.start)
				require.True(t, ok)
				_, ok = g.bucket(g.end)
				require.True(t, ok)
				_, ok = g.bucket(g.end + 1)
				require.False(t, ok)
			} else {
				_, ok := g.bucket(g.start)
				require.False(t, ok)
				i, ok := g.bucket(g.start + 1)
				require.True(t, ok)
				require.Zero(t, i)
				i, ok = g.bucket(g.start + g.step)
				require.True(t, ok)
				require.Zero(t, i)
				i, ok = g.bucket(g.start + g.step + 1)
				require.True(t, ok)
				require.Equal(t, 1, i)
				i, ok = g.bucket(g.end)
				require.True(t, ok)
				require.Equal(t, tt.points-1, i)
				_, ok = g.bucket(g.end + 1)
				require.False(t, ok)
				require.Equal(t, (g.start+g.step)/int64(time.Millisecond), g.steps[0])
			}
		})
	}
}

func TestDefaultMetricsStep(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_010, 0)
	for _, tt := range []struct{ window, want time.Duration }{
		{time.Second, 50 * time.Millisecond},
		{30 * time.Second, 100 * time.Millisecond},
		{time.Minute, time.Second},
		{time.Hour, 15 * time.Second},
		{24 * time.Hour, 5 * time.Minute},
	} {
		t.Run(tt.window.String(), func(t *testing.T) { require.Equal(t, tt.want, DefaultMetricsStep(base, base.Add(tt.window))) })
	}
}

func FuzzMetricsGrid(f *testing.F) {
	f.Add(int64(1_700_000_010_000_000_000), int64(1_700_000_070_000_000_000), int64(15*time.Second))
	f.Add(int64(0), int64(time.Second), int64(time.Millisecond))
	f.Add(int64(math.MaxInt64-20), int64(math.MaxInt64-1), int64(time.Second))
	f.Fuzz(func(t *testing.T, start, end, step int64) {
		g, err := newMetricsGrid(MetricsEvalParams{Start: time.Unix(0, start), End: time.Unix(0, end), Step: time.Duration(step)}, 128)
		if err != nil {
			return
		}
		require.NotEmpty(t, g.steps)
		require.LessOrEqual(t, len(g.steps), 128)
		for i := range g.steps {
			edge := g.start + int64(i+1)*g.step
			bucket, ok := g.bucket(edge)
			require.True(t, ok)
			require.Equal(t, i, bucket)
			require.Equal(t, edge/int64(time.Millisecond), g.steps[i])
		}
	})
}
