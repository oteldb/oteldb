package prompb_test

import (
	"fmt"
	"math"
	"testing"

	nativeprompb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
)

func equalWriteRequest(t *testing.T, a nativeprompb.WriteRequest, b prompb.WriteRequest) {
	t.Helper()

	equalSlices(t, a.Timeseries, b.Timeseries, equalTimeseries)
}

func equalTimeseries(t *testing.T, a nativeprompb.TimeSeries, b prompb.TimeSeries) {
	t.Helper()

	equalSlices(t, a.Labels, b.Labels, equalLabels)
	equalSlices(t, a.Samples, b.Samples, equalSamples)
	equalSlices(t, a.Exemplars, b.Exemplars, equalExemplars)
	equalSlices(t, a.Histograms, b.Histograms, equalHistograms)
}

func equalLabels(t *testing.T, a nativeprompb.Label, b prompb.Label) {
	t.Helper()

	assert.Equal(t, a.Name, string(b.Name))
	assert.Equal(t, a.Value, string(b.Value))
}

func equalSamples(t *testing.T, a nativeprompb.Sample, b prompb.Sample) {
	t.Helper()

	equalFloat(t, a.Value, b.Value, "value")
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

func equalExemplars(t *testing.T, a nativeprompb.Exemplar, b prompb.Exemplar) {
	t.Helper()

	equalSlices(t, a.Labels, b.Labels, equalLabels)
	equalFloat(t, a.Value, b.Value, "value")
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

func equalHistograms(t *testing.T, a nativeprompb.Histogram, b prompb.Histogram) {
	t.Helper()

	// Both counts are proto3 oneofs, so an unset arm is legal: a custom-bucket histogram
	// carries no zero count at all.
	switch c := a.Count.(type) {
	case *nativeprompb.Histogram_CountInt:
		bval, ok := b.Count.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, c.CountInt, bval)
	case *nativeprompb.Histogram_CountFloat:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok)
		equalFloat(t, c.CountFloat, bval, "count_float")
	case nil:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok, "an unset count decodes as the float arm's zero value")
		assert.Zero(t, bval)
	default:
		t.Fatalf("unexpected type %T", c)
	}
	equalFloat(t, a.Sum, b.Sum, "sum")
	assert.Equal(t, a.Schema, b.Schema)
	equalFloat(t, a.ZeroThreshold, b.ZeroThreshold, "zero_threshold")
	switch zc := a.ZeroCount.(type) {
	case *nativeprompb.Histogram_ZeroCountInt:
		bval, ok := b.ZeroCount.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, zc.ZeroCountInt, bval)
	case *nativeprompb.Histogram_ZeroCountFloat:
		bval, ok := b.ZeroCount.AsFloat64()
		assert.True(t, ok)
		equalFloat(t, zc.ZeroCountFloat, bval, "zero_count_float")
	case nil:
		bval, ok := b.ZeroCount.AsFloat64()
		assert.True(t, ok)
		assert.Zero(t, bval)
	default:
		t.Fatalf("unexpected type %T", zc)
	}

	equalSlices(t, a.NegativeSpans, b.NegativeSpans, equalBucketSpans)
	assert.Equal(t, a.NegativeDeltas, b.NegativeDeltas)
	equalFloats(t, a.NegativeCounts, b.NegativeCounts, "negative_counts")

	equalSlices(t, a.PositiveSpans, b.PositiveSpans, equalBucketSpans)
	assert.Equal(t, a.PositiveDeltas, b.PositiveDeltas)
	equalFloats(t, a.PositiveCounts, b.PositiveCounts, "positive_counts")

	assert.Equal(t, int32(a.ResetHint), int32(b.ResetHint))
	assert.Equal(t, a.Timestamp, b.Timestamp)
	equalFloats(t, a.CustomValues, b.CustomValues, "custom_values")
}

// equalFloat compares two floats by bit pattern. Remote write carries NaN payloads with meaning
// — a staleness marker is the specific NaN 0x7ff0000000000002 — so a decoder must preserve the
// bits, and NaN != NaN makes the ordinary comparison useless here.
func equalFloat(t *testing.T, a, b float64, msg string) {
	t.Helper()

	assert.Equal(t, math.Float64bits(a), math.Float64bits(b), msg)
}

func equalFloats(t *testing.T, a, b []float64, msg string) {
	t.Helper()

	require.Len(t, b, len(a), msg)
	for i := range a {
		equalFloat(t, a[i], b[i], fmt.Sprintf("%s[%d]", msg, i))
	}
}

func equalBucketSpans(t *testing.T, a nativeprompb.BucketSpan, b prompb.BucketSpan) {
	t.Helper()

	assert.Equal(t, a.Offset, b.Offset)
	assert.Equal(t, a.Length, b.Length)
}

func equalSlices[A, B any](t *testing.T, a []A, b []B, cmp func(*testing.T, A, B)) {
	t.Helper()

	require.Len(t, b, len(a))
	for i, aElem := range a {
		cmp(t, aElem, b[i])
	}
}

var writeRequestTests = []nativeprompb.WriteRequest{
	{},
	{
		Timeseries: []nativeprompb.TimeSeries{
			{
				Labels: []nativeprompb.Label{
					{Name: "foo", Value: "bar"},
					{Name: "far", Value: "boo"},
				},
				Samples: []nativeprompb.Sample{
					{Value: 1.3, Timestamp: 10},
					{Value: 2.3, Timestamp: 11},
				},
				Exemplars: []nativeprompb.Exemplar{
					{
						Labels: []nativeprompb.Label{
							{Name: "exemplar", Value: "label"},
						},
						Value:     3.14,
						Timestamp: 10,
					},
				},
			},
			{
				Labels: []nativeprompb.Label{
					{Name: "test", Value: "test"},
				},
				Histograms: []nativeprompb.Histogram{
					{
						Count:          &nativeprompb.Histogram_CountFloat{CountFloat: 3.14},
						Sum:            3.14,
						Schema:         4,
						ZeroThreshold:  3.14,
						ZeroCount:      &nativeprompb.Histogram_ZeroCountFloat{ZeroCountFloat: 3.14},
						NegativeSpans:  []nativeprompb.BucketSpan{{Offset: 10, Length: 10}},
						NegativeDeltas: nil,
						NegativeCounts: []float64{1, 2, 3},
						PositiveSpans:  []nativeprompb.BucketSpan{{Offset: 11, Length: 11}},
						PositiveDeltas: nil,
						PositiveCounts: []float64{3, 2, 1},
						ResetHint:      nativeprompb.Histogram_GAUGE,
						Timestamp:      10,
					},
					{
						Count:          &nativeprompb.Histogram_CountInt{CountInt: 3},
						Sum:            3.14,
						Schema:         4,
						ZeroThreshold:  3.14,
						ZeroCount:      &nativeprompb.Histogram_ZeroCountInt{ZeroCountInt: 3},
						NegativeSpans:  []nativeprompb.BucketSpan{{Offset: 10, Length: 10}},
						NegativeDeltas: []int64{1, 2, 3},
						NegativeCounts: nil,
						PositiveSpans:  []nativeprompb.BucketSpan{{Offset: 11, Length: 11}},
						PositiveDeltas: []int64{3, 2, 1},
						PositiveCounts: nil,
						ResetHint:      nativeprompb.Histogram_GAUGE,
						Timestamp:      10,
					},
					// A custom-bucket histogram: bounds are explicit and only the positive side
					// is populated.
					{
						Count:          &nativeprompb.Histogram_CountInt{CountInt: 6},
						Sum:            21,
						Schema:         -53,
						PositiveSpans:  []nativeprompb.BucketSpan{{Offset: 0, Length: 3}},
						PositiveDeltas: []int64{1, 1, 1},
						CustomValues:   []float64{0.5, 1, 2.5},
						Timestamp:      12,
					},
				},
			},
		},
	},
}

func TestWriteRequest(t *testing.T) {
	for i, req := range writeRequestTests {
		native := req
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data, err := native.Marshal()
			require.NoError(t, err)

			var target prompb.WriteRequest
			require.NoError(t, target.Unmarshal(data))
			equalWriteRequest(t, native, target)

			// Ensure that Reset works properly and request could be re-used.
			target.Reset()
			require.NoError(t, target.Unmarshal(data))
			equalWriteRequest(t, native, target)
		})
	}
}

func FuzzWriteRequest(f *testing.F) {
	for _, req := range writeRequestTests {
		data, err := req.Marshal()
		require.NoError(f, err)

		f.Add(data)
	}
	// Add some bad messages.
	for _, data := range [][]byte{
		{},
		{10},
	} {
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var native nativeprompb.WriteRequest
		if err := native.Unmarshal(data); err != nil {
			t.Skipf("Invalid input: %+v", err)
			return
		}

		// This decoder is stricter than gogo's, which skips wire types it does not know (the
		// deprecated proto2 groups) instead of refusing them. Refusing is the safer of the two,
		// so a rejected input is not a finding — only decoding it into something other than what
		// gogo decoded it into is.
		var target prompb.WriteRequest
		if err := target.Unmarshal(data); err != nil {
			t.Skipf("Rejected input: %+v", err)
			return
		}
		equalWriteRequest(t, native, target)

		// Ensure that Reset works properly and request could be re-used.
		target.Reset()
		require.NoError(t, target.Unmarshal(data))
		equalWriteRequest(t, native, target)
	})
}
