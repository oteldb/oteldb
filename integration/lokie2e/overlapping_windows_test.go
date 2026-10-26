package lokie2e_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestLogQLOverlappingWindowsHTTP(t *testing.T) {
	for _, state := range []string{"head", "parts"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			start := time.Unix(1_700_000_000, 137)
			end := start.Add(9 * time.Second)
			records := []struct {
				offset time.Duration
				body   string
				level  string
			}{
				{-5 * time.Second, "boundary", "info"},
				{-5*time.Second + 1, "é🙂", "warn"},
				{-1500 * time.Millisecond, "x", "info"},
				{0, "", "warn"},
				{500 * time.Millisecond, "abcd", "info"},
				{time.Second, "z", "warn"},
				{1500 * time.Millisecond, "", "info"},
				{5 * time.Second, "tail", "warn"},
				{7250 * time.Millisecond, "", "info"},
			}
			logs := plog.NewLogs()
			resource := logs.ResourceLogs().AppendEmpty()
			resource.Resource().Attributes().PutStr("service.name", "overlap")
			rows := resource.ScopeLogs().AppendEmpty().LogRecords()
			for _, record := range records {
				row := rows.AppendEmpty()
				row.SetTimestamp(pcommon.Timestamp(start.Add(record.offset).UnixNano()))
				row.Body().SetStr(record.body)
				if record.level == "info" {
					row.SetSeverityNumber(plog.SeverityNumberInfo)
				} else {
					row.SetSeverityNumber(plog.SeverityNumberWarn)
				}
			}
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close(context.Background()) })
			backend := storagebackend.New(store)
			require.NoError(t, backend.ConsumeLogs(ctx, logs))
			if state == "parts" {
				require.NoError(t, store.Admin().Flush(ctx, "", signal.Log))
				stats := store.Inspect()
				require.Positive(t, stats.Tenants[0].Signals[0].Parts)
				require.Zero(t, stats.Tenants[0].Signals[0].HeadItems)
			}
			querier := backend.Logs()
			provider := integration.TraceProvider(t)
			for _, optimized := range []bool{false, true} {
				optimizers := logqlengine.DefaultOptimizers()
				if optimized {
					optimizers = append(optimizers, &storagebackend.LogQLOptimizer{})
				}
				client := setupDBWithOptimizers(t, provider, querier, querier, optimizers)
				for _, window := range []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 5 * time.Second} {
					for _, op := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
						for _, label := range []string{"level", "service_name"} {
							text := fmt.Sprintf(`sum by (%s) (%s({service_name="overlap"}[%s]))`, label, op, window)
							t.Run(fmt.Sprintf("optimized=%t/%s", optimized, text), func(t *testing.T) {
								response, err := client.QueryRange(ctx, lokiapi.QueryRangeParams{
									Query: text,
									Start: lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(start))),
									End:   lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(end))),
									Step:  lokiapi.NewOptPrometheusDuration("1s"),
								})
								require.NoError(t, err)
								matrix, ok := response.Data.GetMatrixResult()
								require.True(t, ok)
								want := map[string]map[float64]float64{}
								for tick := start; !tick.After(end); tick = tick.Add(time.Second) {
									for _, record := range records {
										at := start.Add(record.offset)
										if !at.After(tick.Add(-window)) || at.After(tick) {
											continue
										}
										key := "overlap"
										if label == "level" {
											key = record.level
										}
										if want[key] == nil {
											want[key] = map[float64]float64{}
										}
										value := 1.0
										if op == "bytes_over_time" || op == "bytes_rate" {
											value = float64(len(record.body))
										}
										if op == "rate" || op == "bytes_rate" {
											value /= window.Seconds()
										}
										want[key][float64(tick.UnixMilli())/1000] += value
									}
								}
								require.Len(t, matrix.Result, len(want))
								for _, series := range matrix.Result {
									points := want[series.Metric.Value[label]]
									require.Len(t, series.Values, len(points))
									for _, point := range series.Values {
										expected, ok := points[point.T]
										require.True(t, ok)
										value, err := strconv.ParseFloat(point.V, 64)
										require.NoError(t, err)
										require.InDelta(t, expected, value, 1e-9)
									}
								}
							})
						}
					}
				}
			}
		})
	}
}
