package scarecrow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// rendezvousScanner blocks every Scan until `want` of them are in flight at once.
//
// This tests the overlap deterministically rather than by timing: if the two sides of a binary
// operator are evaluated concurrently, both scans arrive and the barrier releases; if they are
// evaluated one after the other, the first waits forever and the test fails on its deadline.
// A timing assertion ("the query took less than 2x the latency") would test the same property
// but flake under load.
type rendezvousScanner struct {
	want  int
	inner map[string][]float64

	mu      sync.Mutex
	arrived int
	release chan struct{}
	once    sync.Once
}

func newRendezvousScanner(want int) *rendezvousScanner {
	return &rendezvousScanner{
		want:    want,
		release: make(chan struct{}),
		inner: map[string][]float64{
			"left":  {1, 2, 3},
			"right": {10, 20, 30},
		},
	}
}

func (s *rendezvousScanner) Close() error { return nil }

func (s *rendezvousScanner) nameOf(ms []*labels.Matcher) string {
	for _, m := range ms {
		if m.Name == "__name__" {
			return m.Value
		}
	}

	return ""
}

func (s *rendezvousScanner) Series(
	_ context.Context, _, _ int64, ms []*labels.Matcher,
) ([]labels.Labels, error) {
	name := s.nameOf(ms)
	if _, ok := s.inner[name]; !ok {
		return nil, nil
	}

	// Schema resolution must not block: it is what starts the producers in the first place.
	return []labels.Labels{labels.FromStrings("__name__", name)}, nil
}

// arrive blocks until `want` scans are in flight, then releases them all.
func (s *rendezvousScanner) arrive(ctx context.Context) error {
	s.mu.Lock()
	s.arrived++
	reached := s.arrived >= s.want
	s.mu.Unlock()

	if reached {
		s.once.Do(func() { close(s.release) })
	}

	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *rendezvousScanner) Scan(
	ctx context.Context, _, _ int64, ms []*labels.Matcher,
) (scarecrow.SeriesIterator, error) {
	if err := s.arrive(ctx); err != nil {
		return nil, err
	}

	name := s.nameOf(ms)

	return &rendezvousIterator{name: name, values: s.inner[name]}, nil
}

type rendezvousIterator struct {
	name   string
	values []float64
	done   bool
	cur    scarecrow.Samples
}

func (it *rendezvousIterator) Close() error { return nil }

func (it *rendezvousIterator) Next(context.Context) (*scarecrow.Samples, error) {
	if it.done || it.values == nil {
		return nil, nil
	}
	it.done = true

	it.cur.Labels = labels.FromStrings("__name__", it.name)
	it.cur.T = []int64{0, 30_000, 60_000}
	it.cur.V = it.values

	return &it.cur, nil
}

// TestBinopSidesOverlap asserts that a vector binop evaluates both sides concurrently. Without
// it the build side would be drained to completion before the streaming side was ever opened,
// and the two storage round trips would serialize — the whole reason [concurrent] starts its
// producer during schema resolution rather than on first Next.
func TestBinopSidesOverlap(t *testing.T) {
	t.Parallel()

	sc := newRendezvousScanner(2)

	e := scarecrow.NewEngine(scarecrow.Opts{
		NewScanner: func(storage.Queryable) scarecrow.Scanner { return sc },
	})

	// The deadline is the failure mode: serial evaluation never reaches the barrier.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q, err := e.NewInstantQuery(ctx, nil, nil, `left + on() right`, time.Unix(60, 0))
	require.NoError(t, err)

	defer q.Close()

	res := q.Exec(ctx)
	require.NoError(t, res.Err, "both sides must be scanned concurrently")

	v, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.InDelta(t, 33.0, v[0].F, 0)
}
