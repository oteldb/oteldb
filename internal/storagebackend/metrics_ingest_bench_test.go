package storagebackend_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

func BenchmarkConsumeMetrics(b *testing.B) {
	const points = 10000
	for _, fragmented := range []bool{false, true} {
		b.Run("fragmented="+strconv.FormatBool(fragmented), func(b *testing.B) {
			md := pmetric.NewMetrics()
			metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			var m pmetric.Metric
			for i := range points {
				if i == 0 || fragmented {
					m = metrics.AppendEmpty()
					m.SetName("node_cpu_seconds_total")
					m.SetEmptyGauge()
				}
				dp := m.Gauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(1_000_000_000))
				dp.SetDoubleValue(float64(i))
				dp.Attributes().PutStr("instance", "host-"+strconv.Itoa(i))
				dp.Attributes().PutStr("mode", "user")
			}
			for _, path := range []string{"adapter", "native"} {
				b.Run(path, func(b *testing.B) {
					dir := b.TempDir()
					be, err := file.New(filepath.Join(dir, "parts"))
					require.NoError(b, err)
					store, err := storage.Open(b.Context(), storage.Options{
						Backend: be, WALDir: filepath.Join(dir, "wal"),
						FlushInterval: -1, FlushThresholdBytes: -1,
					})
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, store.Close(context.Background())) })
					backend := storagebackend.New(store)
					var native metric.Metrics
					require.Zero(b, pdataconv.AppendMetrics(&native, md).Points)
					write := func() error {
						if path == "adapter" {
							return backend.ConsumeMetrics(b.Context(), md)
						}
						_, err := store.WriteMetrics(b.Context(), native)
						return err
					}
					require.NoError(b, write())
					b.ReportAllocs()
					b.SetBytes(points * engine.SampleBytes)
					for b.Loop() {
						if err := write(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
