package prome2e_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

func BenchmarkWritePressureBatchSize(b *testing.B) {
	for _, points := range []int{10000, 65536} {
		fragmented, _ := pressureShapes(b, points, 128)
		var native metric.Metrics
		require.Zero(b, pdataconv.AppendMetrics(&native, fragmented).Points)
		for _, path := range []string{"native", "adapter"} {
			b.Run("points="+strconv.Itoa(points)+"/path="+path, func(b *testing.B) {
				store := pressureStore(b, b.TempDir(), nil)
				backend := storagebackend.New(store)
				write := func() error {
					if path == "adapter" {
						return backend.ConsumeMetrics(b.Context(), fragmented)
					}
					_, err := store.WriteMetrics(b.Context(), native)
					return err
				}
				require.NoError(b, write())
				b.ReportAllocs()
				b.SetBytes(int64(points) * engine.SampleBytes)
				for b.Loop() {
					if err := write(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(points*b.N)/b.Elapsed().Seconds(), "points/s")
			})
		}
	}
}
