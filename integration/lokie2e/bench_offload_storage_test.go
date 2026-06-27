package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkLogQLLineFilterOffload compares LogQL line-filter queries with and without the storage
// line-filter offload (storagebackend.LogQLOptimizer), which pushes `|=` filters into the log fetch
// so non-matching records are dropped before the engine materializes them into entries with label
// sets. The Baseline/Offloaded pairing makes the speedup directly comparable.
func BenchmarkLogQLLineFilterOffload(b *testing.B) {
	ctx := context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 50)
	require.NoError(b, err)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	for _, batch := range set.Batches {
		require.NoError(b, backend.ConsumeLogs(ctx, batch))
	}

	params := logqlengine.EvalParams{
		Start:     set.Start.AsTime(),
		End:       set.End.AsTime(),
		Step:      15 * time.Second,
		Direction: logqlengine.DirectionForward,
		Limit:     1000,
	}

	newEngine := func(offload bool) *logqlengine.Engine {
		opts := logqlengine.Options{ParseOptions: logql.ParseOptions{AllowDots: true}}
		if offload {
			opts.Optimizers = []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}
		}
		engine, err := logqlengine.NewEngine(backend.Logs(), opts)
		require.NoError(b, err)
		return engine
	}

	for _, tt := range []struct {
		name  string
		query string
	}{
		{"LineFilter", `{service_name=~".+"} |= "GET"`},
		{"LineFilterRare", `{service_name=~".+"} |= "DELETE"`},
		{"MetricLineFilter", `count_over_time({service_name=~".+"} |= "GET" [1m])`},
	} {
		for _, mode := range []struct {
			name    string
			offload bool
		}{
			{"Baseline", false},
			{"Offloaded", true},
		} {
			query, err := newEngine(mode.offload).NewQuery(ctx, tt.query)
			require.NoError(b, err)
			b.Run(tt.name+"/"+mode.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := query.Eval(ctx, params); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
