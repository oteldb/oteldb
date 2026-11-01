package scarecrow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func BenchmarkDashboardReplay(b *testing.B) {
	dir := os.Getenv("OTELDB_DASHBOARD_REPLAY_DIR")
	if dir == "" {
		b.Skip("set OTELDB_DASHBOARD_REPLAY_DIR to a captured engine-replay directory")
	}
	scanner, targets := loadReplay(b, dir)
	engine := scarecrow.NewEngine(scarecrow.Opts{
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return scanner },
	})
	for _, steps := range []int64{5, 41} {
		queries := append([]replayTarget(nil), targets...)
		for i := range queries {
			queries[i].Start = queries[i].End - (steps-1)*queries[i].Step
		}
		verifyDashboardReplay(b, engine, scanner, queries)
		require.NoError(b, executeReplayBoard(b.Context(), engine, queries, 1))
		for _, clients := range []int{1, 4} {
			b.Run(fmt.Sprintf("steps=%d/clients=%d", steps, clients), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := executeReplayBoard(b.Context(), engine, queries, clients); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func executeReplayQuery(ctx context.Context, engine *scarecrow.Engine, target replayTarget) (*promql.Result, error) {
	var query promql.Query
	var err error
	if target.Instant {
		query, err = engine.NewInstantQuery(ctx, nil, nil, target.Query, time.Unix(target.End, 0))
	} else {
		query, err = engine.NewRangeQuery(ctx, nil, nil, target.Query,
			time.Unix(target.Start, 0), time.Unix(target.End, 0), time.Duration(target.Step)*time.Second)
	}
	if err != nil {
		return nil, err
	}
	defer query.Close()
	result := query.Exec(ctx)
	return result, result.Err
}

func executeReplayBoard(ctx context.Context, engine *scarecrow.Engine, targets []replayTarget, clients int) error {
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, clients)
	for worker := range clients {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(targets) {
					return
				}
				if _, err := executeReplayQuery(ctx, engine, targets[i]); err != nil {
					errs[worker] = errors.Wrapf(err, "target %d", targets[i].QueryIndex)
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func verifyDashboardReplay(b *testing.B, engine *scarecrow.Engine, scanner *replayScanner, targets []replayTarget) {
	b.Helper()
	reference := promql.NewEngine(promql.EngineOpts{
		MaxSamples: 10_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute,
		EnableAtModifier: true, EnableNegativeOffset: true,
	})
	queryable := scanner.queryable()
	for _, target := range targets {
		expected, err := referenceReplayResult(b.Context(), reference, queryable, target)
		require.NoError(b, err, "reference target %d: %s", target.QueryIndex, target.Query)
		result, err := executeReplayQuery(b.Context(), engine, target)
		require.NoError(b, err, "target %d: %s", target.QueryIndex, target.Query)
		data, err := json.Marshal(result.Value)
		require.NoError(b, err)
		var actual []replaySeries
		require.NoError(b, json.Unmarshal(data, &actual))
		want := canonicalReplaySeries(b, expected)
		got := canonicalReplaySeries(b, actual)
		require.Len(b, got, len(want), "target %d: %s", target.QueryIndex, target.Query)
		for key, points := range want {
			values, ok := got[key]
			require.True(b, ok, "target %d missing %s", target.QueryIndex, key)
			require.Len(b, values, len(points), "target %d: %s", target.QueryIndex, key)
			for i, point := range points {
				require.Equal(b, point.ts, values[i].ts, "target %d: %s", target.QueryIndex, key)
				switch {
				case math.IsNaN(point.value):
					require.True(b, math.IsNaN(values[i].value))
				case math.IsInf(point.value, 0):
					require.Equal(b, point.value, values[i].value)
				default:
					require.InDelta(b, point.value, values[i].value, 1e-9+math.Abs(point.value)*1e-9,
						"target %d: %s at %d", target.QueryIndex, key, point.ts)
				}
			}
		}
	}
}

func referenceReplayResult(ctx context.Context, engine *promql.Engine, queryable storage.Queryable, target replayTarget) ([]replaySeries, error) {
	var query promql.Query
	var err error
	if target.Instant {
		query, err = engine.NewInstantQuery(ctx, queryable, nil, target.Query, time.Unix(target.End, 0))
	} else {
		query, err = engine.NewRangeQuery(ctx, queryable, nil, target.Query,
			time.Unix(target.Start, 0), time.Unix(target.End, 0), time.Duration(target.Step)*time.Second)
	}
	if err != nil {
		return nil, err
	}
	defer query.Close()
	result := query.Exec(ctx)
	if result.Err != nil {
		return nil, result.Err
	}
	data, err := json.Marshal(result.Value)
	if err != nil {
		return nil, err
	}
	var series []replaySeries
	if err := json.Unmarshal(data, &series); err != nil {
		return nil, err
	}
	return series, nil
}

type replayPoint struct {
	ts    int64
	value float64
}

func canonicalReplaySeries(t testing.TB, series []replaySeries) map[string][]replayPoint {
	t.Helper()
	out := make(map[string][]replayPoint, len(series))
	for _, result := range series {
		key, err := json.Marshal(result.Metric)
		require.NoError(t, err)
		values := result.Values
		if result.Value != nil {
			values = [][]json.RawMessage{result.Value}
		}
		points := make([]replayPoint, 0, len(values))
		for _, pair := range values {
			ts, value := replayPair(t, pair)
			points = append(points, replayPoint{ts: ts, value: value})
		}
		_, exists := out[string(key)]
		require.False(t, exists, "duplicate result labels")
		out[string(key)] = points
	}
	return out
}
