package chstorage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	noopmeter "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/globalmetric"
	"github.com/oteldb/oteldb/internal/otelbench"
	"github.com/oteldb/oteldb/internal/prompb"
	prw "github.com/oteldb/oteldb/prometheusremotewrite"
)

func newTestMetricBatch(t *testing.T) *metricsBatch {
	t.Helper()
	var stats inserterStats
	require.NoError(t, stats.Init(noopmeter.NewMeterProvider().Meter("test")))
	return newMetricBatch(&Inserter{
		chLogLevel: zapcore.PanicLevel,
		stats:      stats,
		tracker:    globalmetric.NewNoopTracker(),
	})
}

// TestMetricsBatchHistogramInfBucket verifies the decomposed _bucket{le="+Inf"} series equals the
// datapoint count, i.e. the trailing OTLP overflow bucket is not dropped.
func TestMetricsBatchHistogramInfBucket(t *testing.T) {
	md := pmetric.NewMetrics()
	dp := md.ResourceMetrics().AppendEmpty().
		ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty()
	dp.SetName("test_hist")
	h := dp.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	point := h.DataPoints().AppendEmpty()
	point.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	point.SetCount(10)
	point.SetSum(100)
	point.ExplicitBounds().FromRaw([]float64{1, 5, 10})
	// One more count than bounds: the last (4) is the overflow bucket (> the largest bound).
	point.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})

	batch := newTestMetricBatch(t)
	require.NoError(t, batch.mapMetrics(md))

	// Walk the decomposed samples: _count and the last _bucket (le="+Inf") must both equal 10.
	var (
		count      float64
		haveCount  bool
		lastBucket float64
		haveBucket bool
	)
	c := batch.points
	for i := 0; i < c.value.Rows(); i++ {
		switch metricMapping(c.mapping.Row(i)) {
		case histogramCount:
			count, haveCount = c.value.Row(i), true
		case histogramBucket:
			lastBucket, haveBucket = c.value.Row(i), true
		}
	}
	require.True(t, haveCount, "no _count sample")
	require.True(t, haveBucket, "no _bucket sample")
	require.Equal(t, float64(10), count, "_count")
	require.Equal(t, float64(10), lastBucket, `_bucket{le="+Inf"} must equal the datapoint count`)
}

func Benchmark_metricsBatch(b *testing.B) {
	b.ReportAllocs()

	data, err := os.ReadFile(filepath.Join("testdata", "reqs-1k-zstd.rwq"))
	require.NoError(b, err)

	reader := otelbench.NewReader(bytes.NewReader(data))
	require.True(b, reader.Decode())
	compressed := reader.Data()
	z, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(b, err)
	raw, err := io.ReadAll(z)
	require.NoError(b, err)

	rw := &prompb.WriteRequest{}
	require.NoError(b, rw.Unmarshal(raw))

	timeSeries, err := prw.FromTimeSeries(rw.Timeseries, prw.Settings{TimeThreshold: 1_000_000 * time.Hour})
	require.NoError(b, err)

	meterProvider := noopmeter.NewMeterProvider()
	var stats inserterStats
	require.NoError(b, stats.Init(meterProvider.Meter("test")))

	batch := newMetricBatch(&Inserter{
		chLogLevel: zapcore.PanicLevel,
		stats:      stats,
		tracker:    globalmetric.NewNoopTracker(),
	})
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for b.Loop() {
		batch.Reset()
		require.NoError(b, batch.mapMetrics(timeSeries))
	}
}
