package profileqlengine

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

type seriesQuerier struct {
	mockQuerier
	selectSeries func(context.Context, profilestorage.SelectSeriesParams) ([]int64, error)
}

func (q seriesQuerier) SelectSeries(ctx context.Context, p profilestorage.SelectSeriesParams) ([]int64, error) {
	return q.selectSeries(ctx, p)
}

func TestSelectSeriesCapabilityAndFallback(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_000_000, 0)
	end := start.Add(2500 * time.Millisecond)
	const query = `process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend"}`
	var got profilestorage.SelectSeriesParams
	fast := NewEngine(seriesQuerier{selectSeries: func(_ context.Context, p profilestorage.SelectSeriesParams) ([]int64, error) {
		got = p
		return []int64{3, -1, 0}, nil
	}}, Options{})
	values, err := fast.SelectSeries(t.Context(), query, start, end, time.Second)
	require.NoError(t, err)
	assert.Equal(t, []int64{3, -1, 0}, values)
	assert.Equal(t, "process_cpu", got.Type.Name)
	require.Len(t, got.Matchers, 1)
	assert.Equal(t, start, got.Start)
	assert.Equal(t, end, got.End)
	var windows []profilestorage.SelectProfileParams
	fallback := NewEngine(mockQuerier{selectMerge: func(_ context.Context, p profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
		windows = append(windows, p)
		return &profilestorage.FlameTree{Root: &profilestorage.FlameNode{Total: []int64{3, -1, 0}[len(windows)-1]}}, nil
	}}, Options{})
	legacy, err := fallback.SelectSeries(t.Context(), query, start, end, time.Second)
	require.NoError(t, err)
	assert.Equal(t, values, legacy)
	require.Len(t, windows, 3)
	for i, p := range windows {
		from := start.Add(time.Duration(i) * time.Second)
		to := from.Add(time.Second)
		if to.After(end) {
			to = end
		}
		assert.Equal(t, from, p.Start)
		assert.Equal(t, to.Add(-time.Nanosecond), p.End)
	}
}

func TestSelectSeriesErrors(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_000_000, 0)
	sentinel := errors.New("backend failure")
	for _, tc := range []struct {
		name  string
		query string
		step  time.Duration
		q     profilestorage.Querier
		match error
	}{
		{"parse", "not a profile", time.Second, mockQuerier{}, nil},
		{"step", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", 0, mockQuerier{}, nil},
		{"fast error", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", time.Second, seriesQuerier{selectSeries: func(context.Context, profilestorage.SelectSeriesParams) ([]int64, error) { return nil, sentinel }}, sentinel},
		{"bad length", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", time.Second, seriesQuerier{selectSeries: func(context.Context, profilestorage.SelectSeriesParams) ([]int64, error) { return nil, nil }}, nil},
		{"fallback error", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", time.Second, mockQuerier{selectMerge: func(context.Context, profilestorage.SelectProfileParams) (*profilestorage.FlameTree, error) {
			return nil, sentinel
		}}, sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewEngine(tc.q, Options{}).SelectSeries(t.Context(), tc.query, start, start.Add(time.Second), tc.step)
			require.Error(t, err)
			if tc.match != nil {
				assert.ErrorIs(t, err, tc.match)
			}
		})
	}
}
