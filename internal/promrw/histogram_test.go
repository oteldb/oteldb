package promrw_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/promrw"
)

func intHistogram(schema int32) prompb.Histogram {
	h := prompb.Histogram{
		Schema:        schema,
		Sum:           12.5,
		ZeroThreshold: 0.001,
		Timestamp:     1000,

		NegativeSpans:  []prompb.BucketSpan{{Offset: 1, Length: 1}},
		NegativeDeltas: []int64{2},
		PositiveSpans:  []prompb.BucketSpan{{Offset: 1, Length: 2}},
		PositiveDeltas: []int64{1, 1},
	}
	h.Count.SetInt(8)
	h.ZeroCount.SetInt(3)
	return h
}

func convertOne(t *testing.T, ts prompb.TimeSeries) (_ string, dropped int) {
	t.Helper()

	var conv promrw.Converter
	got, dropped, err := conv.Convert([]prompb.TimeSeries{ts}, promrw.Options{
		TimeThreshold: wideThreshold,
		Now:           time.Unix(0, 0).Add(wideThreshold / 2),
	})
	require.NoError(t, err)
	return dump(got), dropped
}

// TestHistogramDecomposition asserts a native histogram is stored as the classic
// _count/_sum/_bucket series a Prometheus histogram is scraped as, with bounds derived from the
// sparse bucket indices: at schema 0 the base is 2, so bucket index i has le 2^i.
func TestHistogramDecomposition(t *testing.T) {
	got, dropped := convertOne(t, prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: []byte("__name__"), Value: []byte("http_duration")},
			{Name: []byte("job"), Value: []byte("api")},
		},
		Histograms: []prompb.Histogram{intHistogram(0)},
	})

	const want = `resource  {}
 scope    {}
  metric http_duration_count unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=8 {job=api}
  metric http_duration_sum unit= kind=1 temporality=2 monotonic=false
   point start=0 ts=1000000000 value=12.5 {job=api}
  metric http_duration_bucket unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=2 {job=api,le=-1}
  metric http_duration_bucket unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=5 {job=api,le=0.001}
  metric http_duration_bucket unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=6 {job=api,le=2}
  metric http_duration_bucket unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=8 {job=api,le=4}
  metric http_duration_bucket unit= kind=1 temporality=2 monotonic=true
   point start=0 ts=1000000000 value=8 {job=api,le=+Inf}
`

	require.Equal(t, want, got)
	require.Zero(t, dropped)
}

// TestHistogramSpanGaps asserts the span offsets are read as Prometheus encodes them: the first
// offset is the index of the first bucket, and a later span's offset is the gap from the previous
// span's last index.
func TestHistogramSpanGaps(t *testing.T) {
	h := prompb.Histogram{
		Schema:         0,
		Timestamp:      1000,
		PositiveSpans:  []prompb.BucketSpan{{Offset: 0, Length: 1}, {Offset: 2, Length: 1}},
		PositiveDeltas: []int64{1, 0},
	}
	h.Count.SetInt(2)

	got, dropped := convertOne(t, prompb.TimeSeries{
		Labels:     []prompb.Label{{Name: []byte("__name__"), Value: []byte("h")}},
		Histograms: []prompb.Histogram{h},
	})

	// Bucket indices 0 and 3 (0 + 1 + 2), so bounds 2^0 = 1 and 2^3 = 8.
	require.Contains(t, got, "value=1 {le=1}")
	require.Contains(t, got, "value=2 {le=8}")
	require.Contains(t, got, "value=2 {le=+Inf}")
	require.Zero(t, dropped)
}

// TestFloatHistogramBuckets asserts a float histogram's absolute counts are read instead of the
// integer deltas.
func TestFloatHistogramBuckets(t *testing.T) {
	h := prompb.Histogram{
		Schema:         0,
		Timestamp:      1000,
		PositiveSpans:  []prompb.BucketSpan{{Offset: 1, Length: 2}},
		PositiveCounts: []float64{1.5, 2.5},
	}
	h.Count.SetFloat(4)

	got, dropped := convertOne(t, prompb.TimeSeries{
		Labels:     []prompb.Label{{Name: []byte("__name__"), Value: []byte("h")}},
		Histograms: []prompb.Histogram{h},
	})

	require.Contains(t, got, "value=1.5 {le=2}")
	require.Contains(t, got, "value=4 {le=4}")
	require.Zero(t, dropped)
}

// TestHistogramSchemaBounds asserts the bound derivation follows the schema: schema 1 halves the
// exponent step, so index i has le 2^(i/2).
func TestHistogramSchemaBounds(t *testing.T) {
	got, dropped := convertOne(t, prompb.TimeSeries{
		Labels:     []prompb.Label{{Name: []byte("__name__"), Value: []byte("h")}},
		Histograms: []prompb.Histogram{intHistogram(1)},
	})

	require.Contains(t, got, "{le=1.414213562373095}")
	require.Contains(t, got, "{le=2}")
	require.Zero(t, dropped)
}

// TestHistogramSkippedWhenSamplesPresent asserts a series carrying both is stored as its samples,
// matching what the pdata translator did.
func TestHistogramSkippedWhenSamplesPresent(t *testing.T) {
	got, dropped := convertOne(t, prompb.TimeSeries{
		Labels:     []prompb.Label{{Name: []byte("__name__"), Value: []byte("h")}},
		Samples:    []prompb.Sample{{Timestamp: 1000, Value: 1}},
		Histograms: []prompb.Histogram{intHistogram(0)},
	})

	require.NotContains(t, got, "_bucket")
	require.Contains(t, got, "value=1 {}")
	require.Equal(t, 1, dropped, "the histogram the samples displaced is counted")
}
