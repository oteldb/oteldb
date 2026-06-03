package metricscache

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeBlock_Empty(t *testing.T) {
	b, err := EncodeBlock(nil, nil, math.MinInt64, math.MinInt64)
	require.NoError(t, err)
	require.Equal(t, 0, b.Count)

	ts, vals, err := b.Decode()
	require.NoError(t, err)
	require.Empty(t, ts)
	require.Empty(t, vals)
}

func TestEncodeDecodeBlock_Single(t *testing.T) {
	ts := []int64{1_000_000}
	vals := []float64{3.14}

	b, err := EncodeBlock(ts, vals, 1_000_000, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, 1, b.Count)
	require.Equal(t, int64(1_000_000), b.MinTS)
	require.Equal(t, int64(1_000_000), b.MaxTS)

	gotTS, gotVals, err := b.Decode()
	require.NoError(t, err)
	require.Equal(t, ts, gotTS)
	require.Equal(t, vals, gotVals)
}

func TestEncodeDecodeBlock_Regular(t *testing.T) {
	// Regular step-aligned data: all double-deltas are zero.
	n := 1000
	step := int64(60_000) // 1 min
	startTS := int64(1_700_000_000_000)
	ts := make([]int64, n)
	vals := make([]float64, n)
	for i := range n {
		ts[i] = startTS + int64(i)*step
		vals[i] = float64(i) * 0.5
	}

	b, err := EncodeBlock(ts, vals, ts[0], ts[n-1])
	require.NoError(t, err)
	require.Equal(t, n, b.Count)

	gotTS, gotVals, err := b.Decode()
	require.NoError(t, err)
	require.Equal(t, ts, gotTS)
	require.InDeltaSlice(t, vals, gotVals, 1e-12)
}

func TestEncodeDecodeBlock_Irregular(t *testing.T) {
	// Non-uniform intervals to exercise non-zero double-deltas.
	ts := []int64{0, 100, 250, 300, 600, 700}
	vals := []float64{1.0, -2.5, 0.0, math.NaN(), math.Inf(1), 42.0}

	b, err := EncodeBlock(ts, vals, ts[0], ts[len(ts)-1])
	require.NoError(t, err)

	gotTS, gotVals, err := b.Decode()
	require.NoError(t, err)
	require.Equal(t, ts, gotTS)
	require.Len(t, gotVals, len(vals))
	for i, v := range vals {
		if math.IsNaN(v) {
			require.True(t, math.IsNaN(gotVals[i]), "index %d: expected NaN", i)
		} else {
			require.Equal(t, v, gotVals[i], "index %d", i)
		}
	}
}

func TestEncodeDecodeBlock_WatermarkPreserved(t *testing.T) {
	// minTS may extend before first data point (MarkFetched scenario).
	ts := []int64{500, 1000}
	vals := []float64{1.0, 2.0}
	minTS := int64(100) // watermark starts before first data point
	maxTS := int64(2000)

	b, err := EncodeBlock(ts, vals, minTS, maxTS)
	require.NoError(t, err)
	require.Equal(t, minTS, b.MinTS)
	require.Equal(t, maxTS, b.MaxTS)

	gotTS, gotVals, err := b.Decode()
	require.NoError(t, err)
	// ts[0] is stored explicitly in the payload, so reconstruction is independent of minTS.
	require.Equal(t, ts, gotTS)
	require.Equal(t, vals, gotVals)
}

func TestToFromBlock_RoundTrip(t *testing.T) {
	e := NewEntry()
	e.Append([]int64{100, 200, 300, 400}, []float64{1.1, 2.2, 3.3, 4.4}, 500)
	e.MarkFetched(50, 500) // extend watermarks beyond data

	b, err := e.ToBlock()
	require.NoError(t, err)

	e2, err := FromBlock(b)
	require.NoError(t, err)

	// Watermarks must match.
	minTS1, maxTS1 := e.Watermarks()
	minTS2, maxTS2 := e2.Watermarks()
	require.Equal(t, minTS1, minTS2)
	require.Equal(t, maxTS1, maxTS2)

	// Data must match.
	ts1, vals1 := e.Slice(minTS1, maxTS1)
	ts2, vals2 := e2.Slice(minTS2, maxTS2)
	require.Equal(t, ts1, ts2)
	require.Equal(t, vals1, vals2)
}

func TestToFromBlock_EmptyEntry(t *testing.T) {
	// An entry that was MarkFetched but received no data points.
	e := NewEntry()
	e.MarkFetched(1000, 5000)

	b, err := e.ToBlock()
	require.NoError(t, err)
	require.Equal(t, 0, b.Count)

	e2, err := FromBlock(b)
	require.NoError(t, err)

	minTS, maxTS := e2.Watermarks()
	require.Equal(t, int64(1000), minTS)
	require.Equal(t, int64(5000), maxTS)
	require.Equal(t, 0, e2.Len())
}

func BenchmarkEncodeBlock(b *testing.B) {
	n := 10000
	step := int64(60_000)
	startTS := int64(1_700_000_000_000)
	ts := make([]int64, n)
	vals := make([]float64, n)
	for i := range n {
		ts[i] = startTS + int64(i)*step
		vals[i] = float64(i) * 1.5
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := EncodeBlock(ts, vals, ts[0], ts[n-1])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlock(b *testing.B) {
	n := 10000
	step := int64(60_000)
	startTS := int64(1_700_000_000_000)
	ts := make([]int64, n)
	vals := make([]float64, n)
	for i := range n {
		ts[i] = startTS + int64(i)*step
		vals[i] = float64(i) * 1.5
	}
	block, err := EncodeBlock(ts, vals, ts[0], ts[n-1])
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, err := block.Decode()
		if err != nil {
			b.Fatal(err)
		}
	}
}
