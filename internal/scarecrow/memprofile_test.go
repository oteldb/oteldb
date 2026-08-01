package scarecrow_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// The memory profile of the four workload archetypes from docs/promql-engine.md §4.4.
//
// It is a measurement harness, not a test: it runs under SCARECROW_MEM=1 and reports numbers
// rather than asserting them. Absolute figures move with Go versions and machines; what it is
// for is the *shape* — whether a cost grows with matched series, with steps, or with neither —
// which is the thing the design makes claims about and which no unit test checks.
//
// The scanner is synthetic on purpose. Real storage would dominate the measurement with its own
// buffers, and the question here is what the engine retains.

// genScanner synthesizes series without allocating per series: one sample buffer is reused for
// every series, so the scanner itself is O(1) in cardinality and anything that grows with series
// count is the engine's.
type genScanner struct {
	series  int
	samples int
	// stepMs is the spacing of the generated raw samples.
	stepMs int64
	// jobs is how many distinct values the "job" label takes, i.e. the group count of an
	// aggregation by (job).
	jobs int

	scans atomic.Int64
}

func (g *genScanner) Close() error { return nil }

func (g *genScanner) labelsAt(i int) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric",
		"instance", fmt.Sprintf("i%d", i),
		"job", fmt.Sprintf("j%d", i%g.jobs),
	)
}

func (g *genScanner) Series(
	context.Context, int64, int64, []*labels.Matcher,
) ([]labels.Labels, error) {
	out := make([]labels.Labels, g.series)
	for i := range out {
		out[i] = g.labelsAt(i)
	}

	return out, nil
}

// Scan only generates samples inside [mint, maxt], matching the partitioned-storage contract
// M16's time-chunking relies on ("a chunk's fetch touches only its own parts and decodes
// nothing extra" — docs/promql-engine.md §4.4). Without this, a chunked query's per-chunk Scan
// calls would each synthesize the *whole* series regardless of chunk size, hiding chunking's
// fetch-side savings behind a scanner that does not model them.
func (g *genScanner) Scan(
	_ context.Context, mint, maxt int64, _ []*labels.Matcher,
) (scarecrow.SeriesIterator, error) {
	g.scans.Add(1)

	n := g.samples
	if maxt > mint {
		n = min(int((maxt-mint)/g.stepMs)+1, g.samples)
	}

	return &genIterator{g: g, mint: mint, n: n}, nil
}

type genIterator struct {
	g    *genScanner
	mint int64
	n    int
	next int

	cur scarecrow.Samples
	t   []int64
	v   []float64
}

func (it *genIterator) Close() error { return nil }

func (it *genIterator) Next(context.Context) (*scarecrow.Samples, error) {
	if it.next >= it.g.series {
		return nil, nil
	}

	i := it.next
	it.next++

	// Buffers are allocated once and refilled, so the iterator holds one series' samples at a
	// time — the same contract oteldb/storage's fetch seam offers.
	if it.t == nil {
		it.t = make([]int64, it.n)
		it.v = make([]float64, it.n)
	}

	for j := range it.n {
		it.t[j] = it.mint + int64(j)*it.g.stepMs
		it.v[j] = float64(i*it.g.samples + j)
	}

	it.cur.Labels = it.g.labelsAt(i)
	it.cur.T = it.t
	it.cur.V = it.v

	return &it.cur, nil
}

// memReport is what one archetype costs.
type memReport struct {
	// live is the heap still reachable once the result exists — what the query retains.
	live uint64
	// peak is the largest heap seen while it ran, including garbage not yet collected.
	peak uint64
	// total is everything allocated over the query's life; a proxy for GC pressure.
	total uint64
	dur   time.Duration
}

func (r memReport) String() string {
	return fmt.Sprintf("live %7.1f MB   peak %7.1f MB   churn %8.1f MB   %v",
		float64(r.live)/(1<<20), float64(r.peak)/(1<<20), float64(r.total)/(1<<20),
		r.dur.Round(time.Millisecond))
}

// measure runs fn and reports what it cost. Peak is sampled rather than instrumented, which is
// approximate but enough to tell a transient spike from a steady retention.
func measure(t *testing.T, fn func() any) memReport {
	t.Helper()

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var (
		peak  atomic.Uint64
		stop  = make(chan struct{})
		done  = make(chan struct{})
		start = time.Now()
	)

	go func() {
		defer close(done)

		// Sampled on a ticker, not in a busy loop: ReadMemStats stops the world, so polling it
		// as fast as possible does not measure the program, it replaces it.
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()

		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}

			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
		}
	}()

	held := fn()

	dur := time.Since(start)
	close(stop)
	<-done

	// A GC with the result still reachable leaves exactly what the query retains.
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	runtime.KeepAlive(held)

	return memReport{
		live:  after.HeapAlloc,
		peak:  peak.Load(),
		total: after.TotalAlloc - before.TotalAlloc,
		dur:   dur,
	}
}

// runQuery evaluates one query against a synthetic scanner and keeps the result alive.
func runQuery(t *testing.T, g *genScanner, qs string, start, end time.Time, step time.Duration) any {
	t.Helper()

	return runQueryOpts(t, scarecrow.Opts{}, g, qs, start, end, step)
}

// runQueryOpts is [runQuery] with caller-supplied engine options (e.g. ChunkSteps), so a
// workload can compare chunked against unchunked without duplicating the query-execution
// plumbing.
func runQueryOpts(
	t *testing.T, opts scarecrow.Opts, g *genScanner, qs string, start, end time.Time, step time.Duration,
) any {
	t.Helper()

	opts.NewScanner = func(storage.Queryable) scarecrow.Scanner { return g }
	e := scarecrow.NewEngine(opts)

	ctx := context.Background()

	var (
		q   promql.Query
		err error
	)

	if step == 0 {
		q, err = e.NewInstantQuery(ctx, nil, nil, qs, start)
	} else {
		q, err = e.NewRangeQuery(ctx, nil, nil, qs, start, end, step)
	}

	require.NoError(t, err)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	// Deliberately not Closed: the result must stay reachable for the live-heap reading, and
	// Close would recycle the very slices being measured.
	return res.Value
}

// TestMemoryArchetypes measures the four workloads. Sizes are scaled where a faithful one would
// take longer than a test should: the point is the growth shape, and each case is run at two
// sizes along whichever axis is in question so the shape is visible rather than asserted.
func TestMemoryArchetypes(t *testing.T) {
	if os.Getenv("SCARECROW_MEM") == "" {
		t.Skip("set SCARECROW_MEM=1 to run the memory profile")
	}

	const minute = int64(60_000)

	t.Run("A_recording_rule", func(t *testing.T) {
		// High cardinality, one step: sum by (job) (rate(m[5m])).
		for _, series := range []int{100_000, 1_000_000} {
			g := &genScanner{series: series, samples: 20, stepMs: 15_000, jobs: 100}

			r := measure(t, func() any {
				return runQuery(t, g, `sum by (job) (rate(metric[5m]))`,
					time.Unix(3600, 0), time.Time{}, 0)
			})

			t.Logf("A  %8d series, 1 step, 100 groups:  %s", series, r)
		}
	})

	t.Run("B_long_range_few_series", func(t *testing.T) {
		// 30d at 15s is 172,801 steps. Raw samples match the step spacing.
		for _, days := range []int{1, 30} {
			steps := int64(days) * 24 * 60 * 4 // 15s steps
			g := &genScanner{
				series: 10, samples: int(steps) + 1, stepMs: 15_000, jobs: 1,
			}

			r := measure(t, func() any {
				return runQuery(t, g, `metric`,
					time.Unix(0, 0), time.Unix(steps*15, 0), 15*time.Second)
			})

			t.Logf("B  %2dd @15s = %7d steps, 10 series:  %s", days, steps+1, r)
		}
	})

	t.Run("C_small_instant", func(t *testing.T) {
		g := &genScanner{series: 5, samples: 20, stepMs: 15_000, jobs: 1}

		r := measure(t, func() any {
			return runQuery(t, g, `metric`, time.Unix(3600, 0), time.Time{}, 0)
		})

		t.Logf("C  5 series, 1 step:                     %s", r)
	})

	t.Run("D_long_range_aggregation", func(t *testing.T) {
		// 30d at 5m is 8,641 steps; 1h windows. Series count is swept to show whether the
		// engine is flat in cardinality, which is the design's central claim.
		for _, series := range []int{1_000, 5_000} {
			const steps = 8_641
			g := &genScanner{
				series: series, samples: steps, stepMs: 5 * minute, jobs: 1,
			}

			r := measure(t, func() any {
				return runQuery(t, g, `avg(avg_over_time(metric[1h]))`,
					time.Unix(0, 0), time.Unix(steps*300, 0), 5*time.Minute)
			})

			t.Logf("D  %6d series, %d steps, 1 group:  %s", series, steps, r)
		}
	})

	t.Run("D2_ungrouped_long_range", func(t *testing.T) {
		// The same shape without the collapsing aggregation: output cardinality equals input,
		// which is where a series-major engine stops being O(1) in series.
		for _, series := range []int{1_000, 5_000} {
			const steps = 8_641
			g := &genScanner{series: series, samples: steps, stepMs: 5 * minute, jobs: 1}

			r := measure(t, func() any {
				return runQuery(t, g, `avg_over_time(metric[1h])`,
					time.Unix(0, 0), time.Unix(steps*300, 0), 5*time.Minute)
			})

			t.Logf("D2 %6d series, %d steps, no fan-in: %s", series, steps, r)
		}
	})

	t.Run("E_one_to_one_join", func(t *testing.T) {
		// The §4.6 exception: a one-to-one binop buffers its whole build side.
		for _, steps := range []int{241, 8_641} {
			g := &genScanner{series: 5_000, samples: steps, stepMs: 15_000, jobs: 1}

			r := measure(t, func() any {
				return runQuery(t, g, `metric / on(instance, job) metric`,
					time.Unix(0, 0), time.Unix(int64(steps)*15, 0), 15*time.Second)
			})

			t.Logf("E  5000 series, %5d steps, 1:1 join:   %s", steps, r)
		}
	})

	t.Run("F_time_chunking", func(t *testing.T) {
		// M16: the same one-to-one join as E, at enough steps that its O(series × steps) build
		// side is the dominant cost, run once with chunking off (ChunkSteps: -1, §4.4's
		// "not implemented" baseline) and once with a small chunk budget. The chunked peak
		// should stay near one chunk's accumulator regardless of how many chunks that takes;
		// the unchunked peak scales with the whole range, exactly the shape M16 exists to cap.
		const (
			series = 500
			steps  = 50_000
		)

		g := &genScanner{series: series, samples: steps, stepMs: 15_000, jobs: 1}
		end := time.Unix(int64(steps)*15, 0)

		unchunked := measure(t, func() any {
			return runQueryOpts(t, scarecrow.Opts{ChunkSteps: -1}, g,
				`metric / on(instance, job) metric`, time.Unix(0, 0), end, 15*time.Second)
		})
		t.Logf("F  %d series, %d steps, 1:1 join, unchunked:      %s", series, steps, unchunked)

		chunked := measure(t, func() any {
			return runQueryOpts(t, scarecrow.Opts{ChunkSteps: 2_000}, g,
				`metric / on(instance, job) metric`, time.Unix(0, 0), end, 15*time.Second)
		})
		t.Logf("F  %d series, %d steps, 1:1 join, chunked(2000): %s", series, steps, chunked)
	})
}
