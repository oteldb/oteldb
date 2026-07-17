package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// BenchmarkLogQLLineFilterOffload compares LogQL line-filter queries with and without the storage
// line-filter offload (storagebackend.LogQLOptimizer). The offload does two things: it pushes `|=`
// into the log fetch as an exact per-record Match, so non-matching records are dropped before the
// engine materializes them into entries with label sets; and it attaches a bloom token hint, so a
// part whose body bloom lacks a required token is skipped without being scanned.
//
// The cases separate the two effects. A bare-word filter yields no token hint — its edge tokens may
// be fragments of a larger token in the body ("GET" occurs inside "xGETy"), so prefiltering on them
// could drop a real match — and therefore measures the per-record offload alone. A delimiter-bounded
// filter also prunes parts. The Baseline/Offloaded pairing makes the speedup directly comparable.
func BenchmarkLogQLLineFilterOffload(b *testing.B) {
	ctx := context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 50)
	require.NoError(b, err)

	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })
	backend := storagebackend.New(store)
	// Flush into several immutable parts: a token hint prunes whole parts, so an unflushed head
	// (one in-memory block) would hide the effect entirely.
	perPart := max(len(set.Batches)/10, 1)
	for i, batch := range set.Batches {
		require.NoError(b, backend.ConsumeLogs(ctx, batch))
		if (i+1)%perPart == 0 {
			require.NoError(b, store.Admin().Flush(ctx, "default", signal.Log))
		}
	}
	require.NoError(b, store.Admin().Flush(ctx, "default", signal.Log))

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
		// Status 500 is ~1.6% of the lines and the only place the token "500" occurs (the byte count
		// is a constant 250), and those lines are contiguous, so the hint prunes nearly every part.
		{"PrunableRare", `{service_name=~".+"} |= " 500 "`},
		// A token no part holds: every part is pruned without being scanned.
		{"PrunableAbsent", `{service_name=~".+"} |= " 599 "`},
		// Same intent and result set as PrunableRare, but a bare word yields no hint and cannot
		// prune — this is what the no-false-negatives guarantee costs on a single-word filter.
		{"SingleWordNoHint", `{service_name=~".+"} |= "500"`},
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
