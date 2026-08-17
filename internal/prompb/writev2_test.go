package prompb_test

import (
	"fmt"
	"testing"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
)

func equalRequestV2(t *testing.T, a writev2.Request, b prompb.WriteRequestV2) {
	t.Helper()

	require.Len(t, b.Symbols, len(a.Symbols))
	for i, sym := range a.Symbols {
		assert.Equal(t, sym, string(b.Symbols[i]), "symbol %d", i)
	}

	equalSlices(t, a.Timeseries, b.Timeseries, equalTimeseriesV2)
}

func equalTimeseriesV2(t *testing.T, a writev2.TimeSeries, b prompb.TimeSeriesV2) {
	t.Helper()

	assert.Equal(t, a.LabelsRefs, b.LabelsRefs)
	equalSlices(t, a.Samples, b.Samples, equalSampleV2)
	equalSlices(t, a.Histograms, b.Histograms, equalHistogramV2)
	equalSlices(t, a.Exemplars, b.Exemplars, equalExemplarV2)

	assert.Equal(t, int32(a.Metadata.Type), int32(b.Metadata.Type))
	assert.Equal(t, a.Metadata.HelpRef, b.Metadata.HelpRef)
	assert.Equal(t, a.Metadata.UnitRef, b.Metadata.UnitRef)
}

func equalSampleV2(t *testing.T, a writev2.Sample, b prompb.SampleV2) {
	t.Helper()

	equalFloat(t, a.Value, b.Value, "value")
	assert.Equal(t, a.Timestamp, b.Timestamp)
	assert.Equal(t, a.StartTimestamp, b.StartTimestamp)
}

func equalExemplarV2(t *testing.T, a writev2.Exemplar, b prompb.ExemplarV2) {
	t.Helper()

	assert.Equal(t, a.LabelsRefs, b.LabelsRefs)
	equalFloat(t, a.Value, b.Value, "value")
	assert.Equal(t, a.Timestamp, b.Timestamp)
}

// equalHistogramV2 compares a 2.0 histogram against the shared [prompb.Histogram], which the two
// schemas encode identically apart from the start timestamp 1.0 lacks.
func equalHistogramV2(t *testing.T, a writev2.Histogram, b prompb.Histogram) {
	t.Helper()

	switch c := a.Count.(type) {
	case *writev2.Histogram_CountInt:
		bval, ok := b.Count.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, c.CountInt, bval)
	case *writev2.Histogram_CountFloat:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok)
		equalFloat(t, c.CountFloat, bval, "count_float")
	case nil:
		bval, ok := b.Count.AsFloat64()
		assert.True(t, ok)
		assert.Zero(t, bval)
	default:
		t.Fatalf("unexpected type %T", c)
	}
	equalFloat(t, a.Sum, b.Sum, "sum")
	assert.Equal(t, a.Schema, b.Schema)
	equalFloat(t, a.ZeroThreshold, b.ZeroThreshold, "zero_threshold")
	switch zc := a.ZeroCount.(type) {
	case *writev2.Histogram_ZeroCountInt:
		bval, ok := b.ZeroCount.AsUint64()
		assert.True(t, ok)
		assert.Equal(t, zc.ZeroCountInt, bval)
	case *writev2.Histogram_ZeroCountFloat:
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

	require.Len(t, b.NegativeSpans, len(a.NegativeSpans))
	for i, s := range a.NegativeSpans {
		assert.Equal(t, s.Offset, b.NegativeSpans[i].Offset)
		assert.Equal(t, s.Length, b.NegativeSpans[i].Length)
	}
	assert.Equal(t, a.NegativeDeltas, b.NegativeDeltas)
	equalFloats(t, a.NegativeCounts, b.NegativeCounts, "negative_counts")

	require.Len(t, b.PositiveSpans, len(a.PositiveSpans))
	for i, s := range a.PositiveSpans {
		assert.Equal(t, s.Offset, b.PositiveSpans[i].Offset)
		assert.Equal(t, s.Length, b.PositiveSpans[i].Length)
	}
	assert.Equal(t, a.PositiveDeltas, b.PositiveDeltas)
	equalFloats(t, a.PositiveCounts, b.PositiveCounts, "positive_counts")

	assert.Equal(t, int32(a.ResetHint), int32(b.ResetHint))
	assert.Equal(t, a.Timestamp, b.Timestamp)
	equalFloats(t, a.CustomValues, b.CustomValues, "custom_values")
	assert.Equal(t, a.StartTimestamp, b.StartTimestamp)
}

var requestV2Tests = []writev2.Request{
	{},
	{
		// Element 0 is the empty string by convention, since an unset ref decodes as 0.
		Symbols: []string{"", "__name__", "http_requests_total", "job", "api", "requests", "1"},
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples: []writev2.Sample{
					{Value: 1.5, Timestamp: 10, StartTimestamp: 5},
					{Value: 2.5, Timestamp: 20},
				},
				Exemplars: []writev2.Exemplar{
					{LabelsRefs: []uint32{3, 4}, Value: 3.14, Timestamp: 10},
				},
				Metadata: writev2.Metadata{
					Type:    writev2.Metadata_METRIC_TYPE_COUNTER,
					HelpRef: 5,
					UnitRef: 6,
				},
			},
			{
				LabelsRefs: []uint32{1, 2},
				Histograms: []writev2.Histogram{
					{
						Count:          &writev2.Histogram_CountInt{CountInt: 8},
						Sum:            12.5,
						Schema:         2,
						ZeroThreshold:  0.001,
						ZeroCount:      &writev2.Histogram_ZeroCountInt{ZeroCountInt: 3},
						NegativeSpans:  []writev2.BucketSpan{{Offset: 1, Length: 1}},
						NegativeDeltas: []int64{2},
						PositiveSpans:  []writev2.BucketSpan{{Offset: 1, Length: 2}},
						PositiveDeltas: []int64{1, 1},
						ResetHint:      writev2.Histogram_RESET_HINT_GAUGE,
						Timestamp:      30,
						StartTimestamp: 25,
					},
					{
						Count:          &writev2.Histogram_CountInt{CountInt: 6},
						Sum:            21,
						Schema:         -53,
						PositiveSpans:  []writev2.BucketSpan{{Offset: 0, Length: 3}},
						PositiveDeltas: []int64{1, 1, 1},
						CustomValues:   []float64{0.5, 1, 2.5},
						Timestamp:      40,
					},
				},
				Metadata: writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_HISTOGRAM},
			},
		},
	},
}

func TestWriteRequestV2(t *testing.T) {
	for i, req := range requestV2Tests {
		native := req
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data, err := native.Marshal()
			require.NoError(t, err)

			var target prompb.WriteRequestV2
			require.NoError(t, target.Unmarshal(data))
			equalRequestV2(t, native, target)

			target.Reset()
			require.NoError(t, target.Unmarshal(data))
			equalRequestV2(t, native, target)
		})
	}
}

// TestWriteRequestV2SchemasDoNotCross asserts the two schemas cannot be confused for one another:
// 2.0 reserves the fields 1.0 puts its timeseries in, so reading a body with the wrong decoder
// yields an empty message rather than plausible nonsense.
func TestWriteRequestV2SchemasDoNotCross(t *testing.T) {
	v1, err := requestV2Tests[1].Marshal()
	require.NoError(t, err)

	var asV1 prompb.WriteRequest
	require.NoError(t, asV1.Unmarshal(v1))
	require.Empty(t, asV1.Timeseries)

	native, err := (&writev2.Request{}).Marshal()
	require.NoError(t, err)

	var asV2 prompb.WriteRequestV2
	require.NoError(t, asV2.Unmarshal(native))
	require.Empty(t, asV2.Timeseries)
}

// TestAppendLabels asserts refs are resolved as index pairs, and that a list the symbol table
// cannot satisfy is refused instead of read out of bounds.
func TestAppendLabels(t *testing.T) {
	symbols := [][]byte{[]byte(""), []byte("__name__"), []byte("m"), []byte("job")}

	got, err := prompb.AppendLabels(nil, []uint32{1, 2, 3, 0}, symbols)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "__name__", string(got[0].Name))
	require.Equal(t, "m", string(got[0].Value))
	require.Equal(t, "job", string(got[1].Name))
	require.Empty(t, got[1].Value)

	_, err = prompb.AppendLabels(nil, []uint32{1}, symbols)
	require.Error(t, err, "an odd ref list is not label pairs")

	_, err = prompb.AppendLabels(nil, []uint32{1, 99}, symbols)
	require.Error(t, err, "a ref past the table is refused")
}

func FuzzWriteRequestV2(f *testing.F) {
	for _, req := range requestV2Tests {
		data, err := req.Marshal()
		require.NoError(f, err)

		f.Add(data)
	}
	for _, data := range [][]byte{{}, {10}, {0x22}, {0x2a}} {
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var native writev2.Request
		if err := native.Unmarshal(data); err != nil {
			t.Skipf("Invalid input: %+v", err)
			return
		}

		// As in FuzzWriteRequest, this decoder is stricter than gogo's about unknown wire types, so
		// a refusal is not a finding.
		var target prompb.WriteRequestV2
		if err := target.Unmarshal(data); err != nil {
			t.Skipf("Rejected input: %+v", err)
			return
		}
		equalRequestV2(t, native, target)

		target.Reset()
		require.NoError(t, target.Unmarshal(data))
		equalRequestV2(t, native, target)
	})
}
