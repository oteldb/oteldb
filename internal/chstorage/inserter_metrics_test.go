package chstorage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	noopmeter "go.opentelemetry.io/otel/metric/noop"

	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/otelbench"
	prw "github.com/go-faster/oteldb/internal/otelreceiver/prometheusremotewrite"
	"github.com/go-faster/oteldb/internal/prompb"
)

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

	timeSeries, err := prw.FromTimeSeries(rw.Timeseries, prw.Settings{TimeThreshold: 1_000_000})
	require.NoError(b, err)

	meterProvider := noopmeter.NewMeterProvider()
	var stats inserterStats
	require.NoError(b, stats.Init(meterProvider.Meter("test")))

	batch := newMetricBatch(stats, globalmetric.NewNoopTracker())
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for b.Loop() {
		batch.Reset()
		require.NoError(b, batch.mapMetrics(timeSeries))
	}
}
