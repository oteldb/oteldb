package metricscache

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

type sourceOfTruth struct {
	data map[Key]seriesData
}

type seriesData struct {
	ts   []int64
	vals []float64
}

func (s *sourceOfTruth) Fetch(key Key, start, end int64) (tss []int64, vals []float64) {
	d, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	for i, t := range d.ts {
		if t >= start && t <= end {
			tss = append(tss, t)
			vals = append(vals, d.vals[i])
		}
	}
	return tss, vals
}

// TestCacheStress hammers the cache from many goroutines, mirroring the querier's
// fill-the-gap access pattern (Lookup → fetch missing tail → Update → Read), and
// asserts that what the cache returns always equals the source of truth.
//
// Two properties of the real workload are reproduced deliberately:
//
//   - Query bounds are aligned to the series step. PromQL queries hit the cache on
//     the step grid, and the cache's watermark gap-detection assumes aligned bounds;
//     feeding arbitrary (unaligned) bounds is misuse that can create false coverage.
//   - Each key is written by a single worker. The cache is a lossy, single-region
//     store: Update may reset an entry when a fetch does not adjoin the cached range,
//     so a concurrent writer could legitimately drop another's just-written coverage.
//     Partitioning keys per worker keeps every series single-writer, which is the only
//     regime where "read back exactly what the source of truth holds" is guaranteed.
func TestCacheStress(t *testing.T) {
	ms, err := NewMemoryStore(10 * 1024 * 1024)
	if err != nil {
		t.Fatal(err)
	}

	seed := time.Now().UnixNano()
	t.Logf("Random seed: %d", seed)
	r := rand.New(rand.NewSource(seed))

	c, err := New(Options{Store: ms})
	if err != nil {
		t.Fatal(err)
	}

	const (
		step    = int64(1000)
		maxTime = int64(100000)
		numKeys = 100
	)

	sot := &sourceOfTruth{data: make(map[Key]seriesData)}
	keys := make([]Key, numKeys)
	for i := range numKeys {
		key := Key{
			Hash: [16]byte{byte(i)},
			Step: step,
			Fn:   "sum",
		}
		keys[i] = key

		var (
			tss  []int64
			vals []float64
		)
		for timestamp := int64(0); timestamp < maxTime; timestamp += step {
			// Random gaps: 20% chance to skip a point.
			if r.Float64() < 0.2 {
				continue
			}
			tss = append(tss, timestamp)
			vals = append(vals, r.Float64())
		}
		sot.data[key] = seriesData{ts: tss, vals: vals}
	}

	const (
		numWorkers     = 50
		queriesPerWork = 200
		keysPerWorker  = numKeys / numWorkers
	)

	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			lr := rand.New(rand.NewSource(seed + int64(workerID)))

			// Single-writer per key: no cross-worker clobber.
			workerKeys := keys[workerID*keysPerWorker : (workerID+1)*keysPerWorker]

			for range queriesPerWork {
				key := workerKeys[lr.Intn(len(workerKeys))]
				// Step-aligned bounds, as PromQL queries the cache.
				start := lr.Int63n(maxTime/step) * step
				end := start + lr.Int63n(maxTime/step-start/step)*step

				maxTS, hit := c.Lookup(key, start)
				fetchStart := start
				if hit {
					// Incrementally fetch only the tail past the watermark.
					fetchStart = nextAlignedFetch(maxTS, key.Step)
				}

				if fetchStart <= end {
					fetchedTS, fetchedVals := sot.Fetch(key, fetchStart, end)
					c.Update(key, Update{
						FetchFrom: fetchStart,
						UntilMs:   end,
						TS:        fetchedTS,
						Vals:      fetchedVals,
					})
				}

				gotTS, gotVals, _ := c.Read(key, start, end)
				wantTS, wantVals := sot.Fetch(key, start, end)

				if !reflect.DeepEqual(gotTS, wantTS) {
					t.Errorf("Timestamp mismatch for key %v [%d, %d]\n want: %v\n got:  %v", key, start, end, wantTS, gotTS)
					return
				}
				if !reflect.DeepEqual(gotVals, wantVals) {
					t.Errorf("Value mismatch for key %v [%d, %d]\n want: %v\n got:  %v", key, start, end, wantVals, gotVals)
					return
				}
			}
		}(w)
	}

	wg.Wait()
}
