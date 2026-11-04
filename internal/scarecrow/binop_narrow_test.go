package scarecrow_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

const binopNarrowCorpus = `
load 10s
  join_left{l="a.b",id="one"} 1 2 3
  join_left{l="a.b",id="two"} 2 3 4
  join_left{l="left-only",id="three"} 3 4 5
  join_right{l="a.b",node="A"} 10 20 30
  join_right{l="axb",node="B"} 20 30 40
  join_right{l="a|b",node="C"} 30 40 50
  join_empty{node="missing"} 2 3 4
  join_empty{l="",node="empty"} 3 4 5
  join_duplicate{l="unused",id="one"} 1 2 3
  join_duplicate{l="unused",id="two"} 2 3 4
  tuple_left{l="a",x="1"} 1 2 3
  tuple_left{l="b",x="2"} 2 3 4
  tuple_right{l="a",x="1"} 2 3 4
  tuple_right{l="b",x="2"} 3 4 5
  tuple_right{l="a",x="2"} 4 5 6
`

type binopScanSpy struct {
	scarecrow.Scanner
	schemas      atomic.Int64
	mu           sync.Mutex
	fetched      map[string]int
	queries      int
	checkSchemas bool
}

func (s *binopScanSpy) Series(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]labels.Labels, error) {
	series, err := s.Scanner.Series(ctx, mint, maxt, matchers)
	s.schemas.Add(1)
	return series, err
}

func (s *binopScanSpy) Scan(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (scarecrow.SeriesIterator, error) {
	if s.checkSchemas && s.schemas.Load() < 2 {
		return nil, errors.New("value scan started before both selector schemas resolved")
	}
	it, err := s.Scanner.Scan(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.queries++
	s.mu.Unlock()
	return &binopScanIterator{SeriesIterator: it, spy: s}, nil
}

type binopScanIterator struct {
	scarecrow.SeriesIterator
	spy *binopScanSpy
}

func (it *binopScanIterator) Next(ctx context.Context) (*scarecrow.Samples, error) {
	s, err := it.SeriesIterator.Next(ctx)
	if s != nil {
		it.spy.mu.Lock()
		it.spy.fetched[s.Labels.Get("__name__")]++
		it.spy.mu.Unlock()
	}
	return s, err
}

func TestBinopSelectorNarrowing(t *testing.T) {
	t.Parallel()
	st := promqltest.LoadedStorage(t, binopNarrowCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	for _, tt := range []struct {
		name, query string
		left, right int
	}{
		{"group left", `join_left * on(l) group_left(node) join_right`, 2, 1},
		{"group right", `join_right * on(l) group_right(node) join_left`, 2, 1},
		{"one to one", `join_left{id="one"} / on(l) join_right`, 1, 1},
		{"comparison", `join_left > on(l) group_left(node) join_right`, 2, 1},
		{"bool", `join_left > bool on(l) group_left(node) join_right`, 2, 1},
		{"regexp literal", `join_left{l="left-only"} * on(l) group_left(node) join_right`, 0, 0},
		{"regexp alternative", `join_right{l="a|b"} * on(l) join_right`, 0, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, instant := range []bool{true, false} {
				spy := &binopScanSpy{Scanner: scarecrow.NewQueryableScanner(st), fetched: make(map[string]int), checkSchemas: true}
				engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return spy }})
				if instant {
					requireSameValue(t, execInstant(t, upstreamEngine(), st, tt.query, time.Unix(20, 0)),
						execInstant(t, engine, st, tt.query, time.Unix(20, 0)))
				} else {
					requireSameValue(t, execRange(t, upstreamEngine(), st, tt.query, time.Unix(0, 0), time.Unix(20, 0), 10*time.Second),
						execRange(t, engine, st, tt.query, time.Unix(0, 0), time.Unix(20, 0), 10*time.Second))
				}
				require.Equal(t, tt.left, spy.fetched["join_left"])
				require.Equal(t, tt.right, spy.fetched["join_right"])
			}
		})
	}
}

func TestBinopSelectorNarrowingFallback(t *testing.T) {
	t.Parallel()
	st := promqltest.LoadedStorage(t, binopNarrowCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	for _, query := range []string{
		`join_left * on(l,id) group_left(node) join_right`,
		`join_left * ignoring(id,node) group_left(node) join_right`,
		`abs(join_left) * on(l) group_left(node) join_right`,
		`join_left and on(l) join_right`,
		`join_left or on(l) join_right`,
		`join_left unless on(l) join_right`,
	} {
		t.Run(query, func(t *testing.T) {
			spy := &binopScanSpy{Scanner: scarecrow.NewQueryableScanner(st), fetched: make(map[string]int)}
			engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return spy }})
			requireSameValue(t, execInstant(t, upstreamEngine(), st, query, time.Unix(20, 0)),
				execInstant(t, engine, st, query, time.Unix(20, 0)))
			require.Equal(t, 3, spy.fetched["join_left"])
			require.Equal(t, 3, spy.fetched["join_right"])
		})
	}
	for _, tt := range []struct {
		query   string
		fetched map[string]int
	}{
		{`join_empty * on(l) group_left(node) join_right`, map[string]int{"join_empty": 2, "join_right": 3}},
		{`join_left * on() group_left(node) join_right{l="a.b"}`, map[string]int{"join_left": 3, "join_right": 1}},
		{`tuple_left * on(l,x) tuple_right`, map[string]int{"tuple_left": 2, "tuple_right": 3}},
	} {
		t.Run(tt.query, func(t *testing.T) {
			spy := &binopScanSpy{Scanner: scarecrow.NewQueryableScanner(st), fetched: make(map[string]int)}
			engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return spy }})
			requireSameValue(t, execInstant(t, upstreamEngine(), st, tt.query, time.Unix(20, 0)),
				execInstant(t, engine, st, tt.query, time.Unix(20, 0)))
			require.Equal(t, tt.fetched, spy.fetched)
		})
	}
}

func TestBinopNarrowingPreservesUnmatchedDuplicateError(t *testing.T) {
	t.Parallel()
	st := promqltest.LoadedStorage(t, binopNarrowCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	spy := &binopScanSpy{Scanner: scarecrow.NewQueryableScanner(st), fetched: make(map[string]int), checkSchemas: true}
	ours := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(storage.Queryable) scarecrow.Scanner { return spy }})
	query := `join_left * on(l) group_left() join_duplicate`
	for _, engine := range []promql.QueryEngine{ours, upstreamEngine()} {
		q, err := engine.NewInstantQuery(t.Context(), st, nil, query, time.Unix(20, 0))
		require.NoError(t, err)
		require.Error(t, q.Exec(t.Context()).Err)
		q.Close()
	}
	require.Zero(t, spy.queries, "the complete schema must be validated before any values are read")
}
