package storagebackend_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func BenchmarkLogQLOverlappingWindows(b *testing.B) {
	ctx := b.Context()
	start := time.Unix(1_700_000_000, 0)
	const span = 10 * time.Minute
	store, err := storage.InMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	if err := backend.ConsumeLogs(ctx, genSpreadLogs(40_000, start, span)); err != nil {
		b.Fatal(err)
	}
	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, op := range []string{"count_over_time", "bytes_over_time"} {
		for _, window := range []string{"1s", "5m"} {
			for _, label := range []string{"level", "service_name"} {
				b.Run(op+"/window="+window+"/by="+label, func(b *testing.B) {
					text := fmt.Sprintf(`sum by (%s) (%s({service_name="api"}[%s]))`, label, op, window)
					b.ReportAllocs()
					for b.Loop() {
						query, err := engine.NewQuery(ctx, text)
						if err != nil {
							b.Fatal(err)
						}
						data, err := query.Eval(ctx, logqlengine.EvalParams{
							Start: start, End: start.Add(span), Step: time.Second,
							Direction: logqlengine.DirectionForward, Limit: -1,
						})
						if err != nil {
							b.Fatal(err)
						}
						if len(data.MatrixResult.Result) == 0 || len(data.MatrixResult.Result[0].Values) < 500 {
							b.Fatal("missing query samples")
						}
					}
				})
			}
		}
	}
}
