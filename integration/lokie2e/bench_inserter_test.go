package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logstorage"
)

type noopClickhouse struct{}

var _ chstorage.ClickHouseClient = (*noopClickhouse)(nil)

func (*noopClickhouse) Do(context.Context, ch.Query) error { return nil }
func (*noopClickhouse) Ping(context.Context) error         { return nil }

// BenchmarkInserterLogs mesaures overhead of logs inserter itself (without Clickhouse part).
func BenchmarkInserterLogs(b *testing.B) {
	ctx := context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 10)
	require.NoError(b, err)

	i, err := chstorage.NewInserter(new(noopClickhouse), chstorage.InserterOptions{
		MeterProvider:  metricnoop.NewMeterProvider(),
		TracerProvider: tracenoop.NewTracerProvider(),
	})
	require.NoError(b, err)
	c, err := logstorage.NewConsumer(i, logstorage.ConsumerOptions{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	var sinkErr error
	for i := 0; i < b.N; i++ {
		for _, batch := range set.Batches {
			sinkErr = c.ConsumeLogs(ctx, batch)
		}
	}
	if sinkErr != nil {
		b.Fatal(sinkErr)
	}
}
