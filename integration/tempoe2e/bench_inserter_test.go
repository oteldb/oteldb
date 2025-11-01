package tempoe2e_test

import (
	"context"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type noopClickhouse struct{}

var _ chstorage.ClickHouseClient = (*noopClickhouse)(nil)

func (*noopClickhouse) Do(context.Context, ch.Query) error { return nil }
func (*noopClickhouse) Ping(context.Context) error         { return nil }

// BenchmarkInserterTraces mesaures overhead of traces inserter itself (without Clickhouse part).
func BenchmarkInserterTraces(b *testing.B) {
	ctx := context.Background()

	set, err := readBatchSet("_testdata/traces.json")
	require.NoError(b, err)

	i, err := chstorage.NewInserter(new(noopClickhouse), chstorage.InserterOptions{
		MeterProvider:  metricnoop.NewMeterProvider(),
		TracerProvider: tracenoop.NewTracerProvider(),
	})
	require.NoError(b, err)
	c := tracestorage.NewConsumer(i)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		for _, batch := range set.Batches {
			sinkErr = c.ConsumeTraces(ctx, batch)
		}
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}
