package storagebackend_test

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	oteldbstorage "github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type serialScarecrowScanner struct{ scarecrow.Scanner }

func (s serialScarecrowScanner) WithQueryBudget(ctx context.Context) context.Context {
	if scanner, ok := s.Scanner.(scarecrow.QueryBudgetScanner); ok {
		return scanner.WithQueryBudget(ctx)
	}
	return ctx
}

func BenchmarkScarecrowSeriesFold(b *testing.B) {
	benchmarkScarecrowSeriesFold(b, 30*time.Minute, 3*time.Second, false)
}

func BenchmarkScarecrowSeriesFoldStale(b *testing.B) {
	benchmarkScarecrowSeriesFold(b, 30*time.Minute, 3*time.Second, true)
}

func BenchmarkScarecrowSeriesFoldControl(b *testing.B) {
	b.Run("instant", func(b *testing.B) { benchmarkScarecrowSeriesFold(b, 0, 0, false) })
	b.Run("short", func(b *testing.B) { benchmarkScarecrowSeriesFold(b, 30*time.Second, 15*time.Second, false) })
}

func BenchmarkScarecrowPredictLinear(b *testing.B) {
	benchmarkScarecrowRangeFold(b, 30*time.Minute, 3*time.Second, false,
		`avg(predict_linear(parallel_gauge[1h], scalar(vector(time()))))`)
}

func benchmarkScarecrowSeriesFold(b *testing.B, duration, interval time.Duration, stale bool) {
	benchmarkScarecrowRangeFold(b, duration, interval, stale, `avg(avg_over_time(parallel_gauge[1h]))`)
}

func benchmarkScarecrowRangeFold(b *testing.B, duration, interval time.Duration, stale bool, expression string) {
	ctx := context.Background()
	store, err := oteldbstorage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, store.Close(ctx)) })
	backend := storagebackend.New(store, storagebackend.WithOverTimePushdown(false))
	base := time.Unix(2_000_000, 0).UTC()
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("parallel_gauge")
	points := metric.SetEmptyGauge().DataPoints()
	const series, samples = 256, 361
	points.EnsureCapacity(series * samples)
	for i := range series {
		for j := range samples {
			point := points.AppendEmpty()
			point.Attributes().PutStr("instance", strconv.Itoa(i))
			point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(j) * 15 * time.Second)))
			point.SetDoubleValue(float64((i*37+j*17)%101) / 7)
			if stale && j%7 == 0 {
				point.SetDoubleValue(math.Float64frombits(0x7ff0000000000002))
			}
		}
	}
	require.NoError(b, backend.ConsumeMetrics(ctx, metrics))
	for _, mode := range []string{"serial", "parallel"} {
		b.Run(mode, func(b *testing.B) {
			engine := scarecrow.NewEngine(scarecrow.Opts{
				NewScanner: func(storage.Queryable) scarecrow.Scanner {
					scanner := backend.ScarecrowScanner()
					if mode == "serial" {
						return serialScarecrowScanner{scanner}
					}
					return scanner
				},
			})
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var query promql.Query
				var err error
				if interval == 0 {
					query, err = engine.NewInstantQuery(ctx, backend, nil,
						expression, base.Add(time.Hour))
				} else {
					query, err = engine.NewRangeQuery(ctx, backend, nil,
						expression,
						base.Add(time.Hour), base.Add(time.Hour+duration), interval)
				}
				require.NoError(b, err)
				result := query.Exec(ctx)
				query.Close()
				require.NoError(b, result.Err)
			}
		})
	}
}
