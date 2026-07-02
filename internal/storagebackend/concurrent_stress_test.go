package storagebackend_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestGroupedCountStableUnderConcurrentMixedLoad reproduces oteldb#1126: an instant
// `count(count(m) by (cpu))` must never transiently return the empty vector while heavy
// range/full-scan/pushdown queries run concurrently. It drives the full production path —
// oteldb PromQL engine (pushdowns included) → storagebackend adapter → embedded storage with a
// deliberately tiny decode cache (constant eviction/recycle churn, the regime of the field
// report) — with concurrent compaction retiring parts mid-flight.
func TestGroupedCountStableUnderConcurrentMixedLoad(t *testing.T) {
	ctx := context.Background()

	store, err := storage.Open(ctx, storage.Options{},
		storage.WithDecodeCache(1<<14), // tiny: constant eviction under concurrent load
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	// node_exporter-shaped data: instances × cpus × modes, several flushed parts + a head tail.
	const (
		instances = 20
		cpus      = 8
		rounds    = 3
		perRound  = 20
		stepMs    = 1000
	)
	modes := []string{"user", "system", "idle", "iowait"}
	base := time.Now().Truncate(time.Minute).Add(-10 * time.Minute)

	for round := range rounds + 1 {
		md := pmetric.NewMetrics()
		for inst := range instances {
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("service.name", "node")
			rm.Resource().Attributes().PutStr("instance", fmt.Sprintf("host-%02d", inst))
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			m.SetName("node_cpu_seconds_total")
			g := m.SetEmptyGauge()
			for cpu := range cpus {
				for _, mode := range modes {
					for k := range perRound {
						dp := g.DataPoints().AppendEmpty()
						at := base.Add(time.Duration(round*perRound+k) * time.Duration(stepMs) * time.Millisecond)
						dp.SetTimestamp(pcommon.Timestamp(at.UnixNano()))
						dp.SetDoubleValue(float64(round*perRound+k) + float64(cpu))
						dp.Attributes().PutStr("cpu", fmt.Sprintf("%d", cpu))
						dp.Attributes().PutStr("mode", mode)
					}
				}
			}
		}
		require.NoError(t, b.ConsumeMetrics(ctx, md))

		if round < rounds {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Metric))
		}
	}

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    50_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	evalAt := base.Add(time.Duration((rounds+1)*perRound) * time.Duration(stepMs) * time.Millisecond)
	rangeStart := evalAt.Add(-1 * time.Minute)

	instant := func(qs string) (int, error) {
		q, err := eng.NewInstantQuery(ctx, b, nil, qs, evalAt)
		if err != nil {
			return 0, err
		}
		defer q.Close()

		res := q.Exec(ctx)
		if res.Err != nil {
			return 0, res.Err
		}

		vec, err := res.Vector()
		if err != nil {
			return 0, err
		}

		return len(vec), nil
	}

	const (
		iters   = 150
		workers = 2 // per background shape
	)

	stop := make(chan struct{})
	fail := make(chan error, workers*4+1)

	var wg sync.WaitGroup
	bg := func(name string, run func() error) {
		for range workers {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					if err := run(); err != nil {
						fail <- fmt.Errorf("%s: %w", name, err)

						return
					}
				}
			})
		}
	}

	// The heavy mix from the field report: matrix irate over a negated matcher, a full-scan
	// ungrouped count (the count pushdown), and an over_time aggregation (its own pushdown).
	bg("irate", func() error {
		q, err := eng.NewRangeQuery(ctx, b, nil,
			`irate(node_cpu_seconds_total{mode!="idle"}[1m])`, rangeStart, evalAt, 5*time.Second)
		if err != nil {
			return err
		}
		defer q.Close()

		res := q.Exec(ctx)

		return res.Err
	})
	bg("full_scan_count", func() error {
		n, err := instant(`count({__name__=~"node_.+"})`)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("full-scan count returned the empty vector")
		}

		return nil
	})
	bg("avg_over_time", func() error {
		_, err := instant(`avg(avg_over_time(node_cpu_seconds_total{mode="user"}[2m])) by (instance)`)

		return err
	})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = store.Admin().Compact(ctx, "default", signal.Metric)
		}
	})

	// The victim query from the field report, checked for exact correctness every iteration.
	var verr error
	for i := 0; i < iters && verr == nil; i++ {
		n, err := instant(`count(count(node_cpu_seconds_total) by (cpu))`)
		if err != nil {
			verr = err

			break
		}
		if n != 1 {
			verr = fmt.Errorf("iter %d: grouped count returned %d samples, want 1 (empty vector regression)", i, n)
		}
	}

	close(stop)
	wg.Wait()
	close(fail)

	require.NoError(t, verr)
	for err := range fail {
		require.NoError(t, err)
	}
}
