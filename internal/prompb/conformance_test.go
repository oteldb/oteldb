package prompb_test

import (
	"fmt"
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

	assert.Equal(t, a.Value, b.Value)
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

func equalExemplars(t *testing.T, a nativeprompb.Exemplar, b prompb.Exemplar) {
	t.Helper()

	equalSlices(t, a.Labels, b.Labels, equalLabels)
	assert.Equal(t, a.Value, b.Value)
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

func equalHistograms(t *testing.T, a nativeprompb.Histogram, b prompb.Histogram) {
	t.Helper()

	switch c := a.Count.(type) {
	case *nativeprompb.Histogram_CountInt:
		bval, ok := b.Count.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, c.CountInt, bval)
	case *nativeprompb.Histogram_CountFloat:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok)
		assert.Equal(t, c.CountFloat, bval)
	default:
		t.Fatalf("unexpected type %T", c)
	}
	assert.Equal(t, a.Sum, b.Sum)
	assert.Equal(t, a.Schema, b.Schema)
	assert.Equal(t, a.ZeroThreshold, b.ZeroThreshold)
	switch zc := a.ZeroCount.(type) {
	case *nativeprompb.Histogram_ZeroCountInt:
		bval, ok := b.Count.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, zc.ZeroCountInt, bval)
	case *nativeprompb.Histogram_ZeroCountFloat:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok)
		assert.Equal(t, zc.ZeroCountFloat, bval)
	default:
		t.Fatalf("unexpected type %T", zc)
	}

	equalSlices(t, a.NegativeSpans, b.NegativeSpans, equalBucketSpans)
	assert.Equal(t, a.NegativeDeltas, b.NegativeDeltas)
	assert.Equal(t, a.NegativeCounts, b.NegativeCounts)

	equalSlices(t, a.PositiveSpans, b.PositiveSpans, equalBucketSpans)
	assert.Equal(t, a.PositiveDeltas, b.PositiveDeltas)
	assert.Equal(t, a.PositiveCounts, b.PositiveCounts)

	assert.Equal(t, int32(a.ResetHint), int32(b.ResetHint))
	assert.Equal(t, a.Timestamp, b.Timestamp)
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

		var target prompb.WriteRequest
		require.NoError(t, target.Unmarshal(data))
		equalWriteRequest(t, native, target)

		// Ensure that Reset works properly and request could be re-used.
		target.Reset()
		require.NoError(t, target.Unmarshal(data))
		equalWriteRequest(t, native, target)
	})
}
