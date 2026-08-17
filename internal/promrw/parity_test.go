package promrw_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/metric"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otelbench"
	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/prometheusremotewrite"
)

// wideThreshold keeps every point of the recorded corpus in window, so the conversion is
// exercised instead of the drop path.
const wideThreshold = 100 * 365 * 24 * time.Hour

func readCorpus(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "prometheusremotewrite", "testdata", "reqs-1k-zstd.rwq"))
	require.NoError(tb, err)

	reader := otelbench.NewReader(bytes.NewReader(data))
	require.True(tb, reader.Decode())

	z, err := zstd.NewReader(bytes.NewReader(reader.Data()))
	require.NoError(tb, err)
	defer z.Close()

	raw, err := io.ReadAll(z)
	require.NoError(tb, err)
	return raw
}

// viaPdata converts through the path the collector pipeline uses today: prompb → pmetric →
// pdataconv → metric.Metrics.
func viaPdata(tb testing.TB, tss []prompb.TimeSeries, threshold time.Duration) *metric.Metrics {
	tb.Helper()

	md, err := prometheusremotewrite.FromTimeSeries(tss, prometheusremotewrite.Settings{
		TimeThreshold: threshold,
	})
	require.NoError(tb, err)

	var batch metric.Metrics
	pdataconv.AppendMetrics(&batch, md)
	return &batch
}

// TestParityWithPdataPath asserts the direct converter agrees with the pdata path over a recorded
// remote write corpus, which is float samples from a real Prometheus.
//
// It is a regression guard on real data, not a specification: the two deliberately disagree where
// the pdata path is wrong — on single-separator counter names, and on native histograms, which the
// corpus does not carry and whose bucket counts the pdata path never reads at all.
func TestParityWithPdataPath(t *testing.T) {
	raw := readCorpus(t)

	var req prompb.WriteRequest
	require.NoError(t, req.Unmarshal(raw))
	require.NotEmpty(t, req.Timeseries)

	var conv promrw.Converter
	got, _, err := conv.Convert(req.Timeseries, promrw.Options{TimeThreshold: wideThreshold})
	require.NoError(t, err)

	require.Equal(t, dump(viaPdata(t, req.Timeseries, wideThreshold)), dump(got))
}

// TestConvertReuse asserts a converter reused across requests yields the same result as a fresh
// one, so the retained arenas hand out distinct storage per request.
func TestConvertReuse(t *testing.T) {
	raw := readCorpus(t)

	var req prompb.WriteRequest
	require.NoError(t, req.Unmarshal(raw))

	var (
		conv promrw.Converter
		want string
	)
	for i := range 3 {
		got, _, err := conv.Convert(req.Timeseries, promrw.Options{TimeThreshold: wideThreshold})
		require.NoError(t, err)

		if i == 0 {
			want = dump(got)
			continue
		}
		require.Equal(t, want, dump(got), "conversion %d", i)
	}
}

func BenchmarkConvert(b *testing.B) {
	raw := readCorpus(b)

	var req prompb.WriteRequest
	require.NoError(b, req.Unmarshal(raw))

	b.Run("Direct", func(b *testing.B) {
		var conv promrw.Converter
		opts := promrw.Options{TimeThreshold: wideThreshold, Now: time.Now()}

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			if _, _, err := conv.Convert(req.Timeseries, opts); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Pdata", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			md, err := prometheusremotewrite.FromTimeSeries(req.Timeseries, prometheusremotewrite.Settings{
				TimeThreshold: wideThreshold,
			})
			if err != nil {
				b.Fatal(err)
			}

			var batch metric.Metrics
			pdataconv.AppendMetrics(&batch, md)
		}
	})
}
