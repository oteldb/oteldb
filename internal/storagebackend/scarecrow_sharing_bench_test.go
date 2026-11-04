package storagebackend_test

import (
	"context"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	nativestorage "github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkNativeQuerySharing uses 64 hosts so a fixed 32-iteration fresh-host run never repeats
// a selector. Longer runs reset Backend caches every 64 panels, retaining library block caches.
// Six panel queries share a denominator or an aggregate grid over reopened file parts.
func BenchmarkNativeQuerySharing(b *testing.B) {
	for _, workload := range []string{"cpu", "common-fold"} {
		b.Run(workload, func(b *testing.B) {
			for _, reuse := range []string{"cached", "fresh-host"} {
				b.Run(reuse, func(b *testing.B) {
					for _, sharing := range []string{"disabled", "enabled"} {
						b.Run(sharing, func(b *testing.B) { benchmarkNativeQuerySharing(b, workload, reuse, sharing == "enabled") })
					}
				})
			}
		})
	}
}

func benchmarkNativeQuerySharing(b *testing.B, workload, reuse string, sharing bool) {
	dir := b.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(b, err)
	open := func() *nativestorage.Storage {
		store, err := nativestorage.Open(b.Context(), nativestorage.Options{}, nativestorage.WithBackend(files),
			nativestorage.WithWALDir(filepath.Join(dir, "wal")), nativestorage.WithFlushInterval(-1),
			nativestorage.WithDecodeCache(8<<20), nativestorage.WithAggregateStats())
		require.NoError(b, err)
		return store
	}
	store := open()
	base := time.Unix(2_000_000, 0).UTC()
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("sharing_cpu_seconds")
	points := metric.SetEmptyGauge().DataPoints()
	points.EnsureCapacity(64 * 6 * 2 * 121)
	for host := range 64 {
		for mode := range 6 {
			for cpu := range 2 {
				for sample := range 121 {
					point := points.AppendEmpty()
					point.Attributes().PutStr("instance", strconv.Itoa(host))
					point.Attributes().PutStr("mode", strconv.Itoa(mode))
					point.Attributes().PutStr("cpu", strconv.Itoa(cpu))
					point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(sample) * 15 * time.Second)))
					point.SetDoubleValue(float64(sample * 15 * (mode + 1)))
				}
			}
		}
	}
	require.NoError(b, storagebackend.New(store).ConsumeMetrics(b.Context(), metrics))
	require.NoError(b, store.Admin().Flush(b.Context(), "", signal.Metric))
	require.NoError(b, store.Close(b.Context()))
	store = open()
	b.Cleanup(func() { require.NoError(b, store.Close(context.WithoutCancel(b.Context()))) })
	var options []storagebackend.Option
	if !sharing {
		options = append(options, storagebackend.WithNativeQuerySharing(0, 0))
	}
	backend := storagebackend.New(store, options...)
	queryEngine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
	verify := true
	panel := func(host int) {
		var wg sync.WaitGroup
		var errs [6]error
		for mode := range 6 {
			wg.Go(func() {
				selector := `sharing_cpu_seconds{instance="` + strconv.Itoa(host) + `"}`
				query := `sum(irate(sharing_cpu_seconds{instance="` + strconv.Itoa(host) + `",mode="` + strconv.Itoa(mode) + `"}[1m])) / scalar(count(count(` + selector + `) by(cpu)))`
				if workload == "common-fold" {
					functions := []string{"sum", "avg", "min", "max", "stddev", "stdvar"}
					query = functions[mode] + `(avg_over_time(` + selector + `[5m]))`
				}
				var q promql.Query
				q, errs[mode] = queryEngine.NewRangeQuery(b.Context(), backend, nil, query, base.Add(10*time.Minute), base.Add(30*time.Minute), 15*time.Second)
				if errs[mode] != nil {
					return
				}
				result := q.Exec(b.Context())
				q.Close()
				errs[mode] = result.Err
				if verify && result.Err == nil {
					matrix, ok := result.Value.(promql.Matrix)
					if !ok || len(matrix) != 1 || len(matrix[0].Floats) != 81 {
						errs[mode] = errors.New("panel fixture returned an empty or unexpected result")
						return
					}
					for _, point := range matrix[0].Floats {
						if math.IsNaN(point.F) || math.IsInf(point.F, 0) || workload == "cpu" && math.Abs(point.F-float64(mode+1)) > 1e-12 {
							errs[mode] = errors.New("panel fixture returned an incorrect or non-finite value")
							return
						}
					}
				}
			})
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(b, err)
		}
	}
	panel(63)
	verify = false
	backend = storagebackend.New(store, options...)
	if reuse == "cached" {
		panel(0)
	}
	b.ReportAllocs()
	iteration := 0
	for b.Loop() {
		if reuse == "fresh-host" && iteration != 0 && iteration%64 == 0 {
			b.StopTimer()
			backend = storagebackend.New(store, options...)
			b.StartTimer()
		}
		host := 0
		if reuse == "fresh-host" {
			host = (iteration*37 + 11) % 64
		}
		panel(host)
		iteration++
	}
}
