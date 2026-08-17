package prometheusremotewrite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/oteldb/internal/prompb"
)

// mapped runs one histogram side through the mapper and returns the dense bucket array it produced,
// with its offset.
func mapped(t *testing.T, hist promHistorgram) (offset int32, counts []uint64) {
	t.Helper()

	buckets := pmetric.NewExponentialHistogramDataPointBuckets()
	mapExpBuckets(hist, buckets)

	return buckets.Offset(), buckets.BucketCounts().AsRaw()
}

// TestMapExpBucketsIntegerDeltas asserts an integer histogram's delta-encoded counts are read at
// all, and accumulated. They live in deltas, which the mapper used to ignore entirely — so every
// integer native histogram, which is what Prometheus sends by default, arrived with no buckets.
func TestMapExpBucketsIntegerDeltas(t *testing.T) {
	offset, counts := mapped(t, promHistorgram{
		spans:  []prompb.BucketSpan{{Offset: 2, Length: 3}},
		deltas: []int64{2, 1, -1},
	})

	require.Equal(t, int32(1), offset, "an OTLP offset is one below the Prometheus index")
	require.Equal(t, []uint64{2, 3, 2}, counts)
}

// TestMapExpBucketsFloatCounts asserts a float histogram's counts are taken as absolute rather than
// accumulated.
func TestMapExpBucketsFloatCounts(t *testing.T) {
	offset, counts := mapped(t, promHistorgram{
		spans:  []prompb.BucketSpan{{Offset: 0, Length: 3}},
		counts: []float64{2, 3, 2},
	})

	require.Equal(t, int32(-1), offset)
	require.Equal(t, []uint64{2, 3, 2}, counts)
}

// TestMapExpBucketsSpanGaps asserts the buckets a gap skips are written out as zero. Prometheus
// omits them; OTLP addresses buckets by position in one dense array, so dropping them shifts every
// later bucket onto the wrong bound.
func TestMapExpBucketsSpanGaps(t *testing.T) {
	offset, counts := mapped(t, promHistorgram{
		// Indices 0, then a gap of two, then 3 and 4.
		spans:  []prompb.BucketSpan{{Offset: 0, Length: 1}, {Offset: 2, Length: 2}},
		deltas: []int64{1, 1, 0},
	})

	require.Equal(t, int32(-1), offset)
	require.Equal(t, []uint64{1, 0, 0, 2, 2}, counts)
}

// TestMapExpBucketsMalformed asserts a histogram the spans and counts disagree about is truncated
// rather than read past the end, and that an impossible count is stored as empty rather than
// becoming a huge bucket.
func TestMapExpBucketsMalformed(t *testing.T) {
	t.Run("NoSpans", func(t *testing.T) {
		offset, counts := mapped(t, promHistorgram{deltas: []int64{1}})
		require.Zero(t, offset)
		require.Empty(t, counts)
	})
	t.Run("NoCounts", func(t *testing.T) {
		_, counts := mapped(t, promHistorgram{spans: []prompb.BucketSpan{{Offset: 0, Length: 3}}})
		require.Empty(t, counts)
	})
	t.Run("FewerCountsThanSpansClaim", func(t *testing.T) {
		_, counts := mapped(t, promHistorgram{
			spans:  []prompb.BucketSpan{{Offset: 0, Length: 5}},
			deltas: []int64{1, 1},
		})
		require.Equal(t, []uint64{1, 2}, counts)
	})
	t.Run("NegativeAccumulatedDelta", func(t *testing.T) {
		_, counts := mapped(t, promHistorgram{
			spans:  []prompb.BucketSpan{{Offset: 0, Length: 2}},
			deltas: []int64{1, -5},
		})
		require.Equal(t, []uint64{1, 0}, counts)
	})
	t.Run("NegativeSpanOffset", func(t *testing.T) {
		_, counts := mapped(t, promHistorgram{
			spans:  []prompb.BucketSpan{{Offset: 0, Length: 1}, {Offset: -3, Length: 1}},
			deltas: []int64{1, 0},
		})
		require.Equal(t, []uint64{1, 1}, counts, "a negative gap fills nothing")
	})
}

// TestFromTimeSeriesIntegerHistogram asserts an integer native histogram reaches the exponential
// histogram with its buckets, end to end. This is the shape Prometheus sends by default, and it
// used to arrive with both sides empty.
func TestFromTimeSeriesIntegerHistogram(t *testing.T) {
	h := prompb.Histogram{
		Schema:         0,
		Sum:            12.5,
		ZeroThreshold:  0.001,
		Timestamp:      time.Now().UnixMilli(),
		NegativeSpans:  []prompb.BucketSpan{{Offset: 1, Length: 1}},
		NegativeDeltas: []int64{2},
		PositiveSpans:  []prompb.BucketSpan{{Offset: 1, Length: 2}},
		PositiveDeltas: []int64{1, 1},
	}
	h.Count.SetInt(8)
	h.ZeroCount.SetInt(3)

	md, err := FromTimeSeries([]prompb.TimeSeries{{
		Labels:     []prompb.Label{{Name: []byte(nameStr), Value: []byte("h")}},
		Histograms: []prompb.Histogram{h},
	}}, Settings{TimeThreshold: 24 * time.Hour})
	require.NoError(t, err)

	metric := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	require.Equal(t, pmetric.MetricTypeExponentialHistogram, metric.Type())

	dp := metric.ExponentialHistogram().DataPoints().At(0)
	require.Equal(t, uint64(8), dp.Count())
	require.Equal(t, uint64(3), dp.ZeroCount())
	require.Equal(t, []uint64{1, 2}, dp.Positive().BucketCounts().AsRaw())
	require.Equal(t, []uint64{2}, dp.Negative().BucketCounts().AsRaw())
}
