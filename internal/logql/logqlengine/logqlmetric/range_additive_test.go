package logqlmetric

import (
	"math"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func TestOverlappingRangeMatchesNaive(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 137)
	end := start.Add(8 * time.Second)
	a := mapLabels(map[string]string{"job": "a", "pod": "one"})
	b := mapLabels(map[string]string{"job": "a", "pod": "two"})
	c := mapLabels(map[string]string{"job": "b", "pod": "three"})
	for _, values := range []struct {
		name   string
		values []float64
	}{
		{"bytes", []float64{7, 0, 3, 0, 11, 1, 0}},
		{"fractional", []float64{0.25, 0.5, 3, 0, 11, 1, 0}},
		{"nonfinite", []float64{math.NaN(), math.Inf(1), math.Inf(-1), -2, 11, 1, 0}},
		{"large", []float64{1 << 62, 1 << 62, 1 << 62, 1, 11, 1, 0}},
	} {
		for _, window := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 5 * time.Second} {
			for _, op := range []logql.RangeOp{logql.RangeOpCount, logql.RangeOpRate, logql.RangeOpBytes, logql.RangeOpBytesRate} {
				for _, grouping := range []struct {
					name string
					g    *logql.Grouping
				}{
					{"stream", nil},
					{"by", &logql.Grouping{Labels: []logql.Label{"job"}}},
					{"without", &logql.Grouping{Without: true, Labels: []logql.Label{"pod"}}},
				} {
					t.Run(values.name+"/"+op.String()+"/"+window.String()+"/"+grouping.name, func(t *testing.T) {
						t.Parallel()
						offsets := []time.Duration{-5 * time.Second, -5*time.Second + 1, 0, time.Second / 2, time.Second, 1500 * time.Millisecond, 7 * time.Second}
						sets := []logqlabels.AggregatedLabels{a, b, c, a, b, a, c}
						samples := make([]SampledEntry, len(offsets))
						for i, offset := range offsets {
							samples[i] = SampledEntry{Timestamp: otelstorage.NewTimestampFromTime(start.Add(offset)), Sample: values.values[i], Set: sets[i]}
						}
						expr := &logql.RangeAggregationExpr{Op: op, Range: logql.LogRangeExpr{Range: window}, Grouping: grouping.g}
						iter, err := RangeAggregation(iterators.Slice(samples), expr, start, end, time.Second)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, iter.Close()) })
						agg, err := buildBatchAggregator(expr)
						require.NoError(t, err)
						var got Step
						for ts := start; !ts.After(end); ts = ts.Add(time.Second) {
							require.True(t, iter.Next(&got))
							require.Equal(t, otelstorage.NewTimestampFromTime(ts), got.Timestamp)
							want := map[logqlabels.GroupingKey][]FPoint{}
							for _, sample := range samples {
								if at := sample.Timestamp.AsTime(); at.After(ts.Add(-window)) && !at.After(ts) {
									set := sample.Set
									if g := grouping.g; g != nil {
										if g.Without {
											set = set.Without(g.Labels...)
										} else {
											set = set.By(g.Labels...)
										}
									}
									key := set.Key()
									want[key] = append(want[key], FPoint{Timestamp: sample.Timestamp, Value: sample.Sample})
								}
							}
							require.Len(t, got.Samples, len(want))
							for _, sample := range got.Samples {
								points, ok := want[sample.Set.Key()]
								require.True(t, ok)
								expected := agg.Aggregate(points)
								switch {
								case math.IsNaN(expected):
									require.True(t, math.IsNaN(sample.Data))
								case math.IsInf(expected, 0):
									require.Equal(t, expected, sample.Data)
								default:
									require.InDelta(t, expected, sample.Data, max(1e-12, math.Abs(expected)*1e-12))
								}
							}
						}
						require.False(t, iter.Next(&got))
						require.NoError(t, iter.Err())
					})
				}
			}
		}
	}
}

func TestAdditiveSeriesBoundsHistory(t *testing.T) {
	t.Parallel()
	const width = 128
	start := time.Unix(1_700_000_000, 0)
	series := &additiveSeries{bytesValid: true}
	for n := range 10_000 {
		ts := start.Add(time.Duration(n) * time.Second)
		series.expire(ts.Add(-width * time.Second))
		series.add(FPoint{Timestamp: otelstorage.NewTimestampFromTime(ts), Value: 1})
		require.Equal(t, min(n+1, width), series.size)
		require.Equal(t, int64(series.size), series.bytes)
		require.LessOrEqual(t, len(series.data), width*4, "the queue must not grow with elapsed steps")
	}
}

func TestAdditiveSeriesGrowsAfterWrap(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	series := &additiveSeries{bytesValid: true}
	appendPoint := func(n int) {
		series.add(FPoint{Timestamp: otelstorage.NewTimestampFromTime(start.Add(time.Duration(n) * time.Second)), Value: float64(n + 1)})
	}
	for n := range 8 {
		appendPoint(n)
	}
	series.expire(start.Add(2 * time.Second))
	for n := 8; n < 12; n++ {
		appendPoint(n)
	}
	require.Equal(t, 9, series.size)
	for n := range series.size {
		index := (series.head + n) % len(series.data)
		require.Equal(t, float64(n+4), series.data[index].Value)
	}
	require.Equal(t, int64(72), series.bytes)
	require.Equal(t, 72.0, series.sum())
	series.expire(start.Add(8 * time.Second))
	require.Equal(t, 3, series.size)
	require.Equal(t, int64(33), series.bytes)
	require.Equal(t, 33.0, series.sum())
}

type failingWindowSource struct {
	err, closeErr error
	calls         int
	closed        bool
}

func (s *failingWindowSource) Next(*SampledEntry) bool { s.calls++; return false }
func (s *failingWindowSource) Err() error              { return s.err }
func (s *failingWindowSource) Close() error            { s.closed = true; return s.closeErr }

func TestAdditiveRangeLifecycle(t *testing.T) {
	t.Parallel()
	failure := errors.New("source unavailable")
	for _, tc := range []struct {
		name     string
		drain    bool
		closeErr error
	}{
		{"source error", true, nil},
		{"early close", false, nil},
		{"close error", false, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &failingWindowSource{err: failure, closeErr: tc.closeErr}
			expr := &logql.RangeAggregationExpr{Op: logql.RangeOpCount, Range: logql.LogRangeExpr{Range: 5 * time.Second}}
			start := time.Unix(1_700_000_000, 0)
			iter, err := RangeAggregation(source, expr, start, start.Add(3*time.Second), time.Second)
			require.NoError(t, err)
			if tc.drain {
				var step Step
				for iter.Next(&step) {
					require.Empty(t, step.Samples)
				}
				require.ErrorIs(t, iter.Err(), failure)
			} else {
				require.Zero(t, source.calls)
			}
			require.Equal(t, tc.closeErr, iter.Close())
			require.True(t, source.closed)
		})
	}
}
