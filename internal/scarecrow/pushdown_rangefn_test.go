package scarecrow_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

type rangeFunctionTestScanner struct {
	scarecrow.Scanner
	grids     []scarecrow.WindowGrid
	err       error
	malformed bool
}

func (*rangeFunctionTestScanner) SupportsRangeFunction(name string) bool { return name == "rate" }
func (s *rangeFunctionTestScanner) RangeFunction(_ context.Context, _ string, grid scarecrow.WindowGrid, _ []*labels.Matcher) ([]scarecrow.Samples, error) {
	s.grids = append(s.grids, grid)
	if s.err != nil {
		return nil, s.err
	}
	result := scarecrow.Samples{Labels: labels.FromStrings("job", "api")}
	for i := range grid.NumSteps {
		result.T = append(result.T, grid.Start+int64(i)*grid.Step)
		result.V = append(result.V, 2)
	}
	if s.malformed {
		result.V = nil
	}
	return []scarecrow.Samples{result}, nil
}

func TestRangeFunctionPushdownLimitsAndErrors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		maxSamples int
		failure    error
		malformed  bool
		want       string
	}{
		{name: "budget", maxSamples: 1, want: "too many samples"},
		{name: "backend", failure: errors.New("backend unavailable"), want: "backend unavailable"},
		{name: "malformed", malformed: true, want: "mismatched samples"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := promqltest.LoadedStorage(t, twoSeries)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			scanner := &rangeFunctionTestScanner{Scanner: scarecrow.NewQueryableScanner(store), err: tt.failure, malformed: tt.malformed}
			engine := scarecrow.NewEngine(scarecrow.Opts{MaxSamples: tt.maxSamples, NewScanner: func(storage.Queryable) scarecrow.Scanner { return scanner }})
			query, err := engine.NewRangeQuery(t.Context(), store, nil, "rate(http_requests[1m])", time.Unix(0, 0), time.Unix(120, 0), time.Minute)
			require.NoError(t, err)
			t.Cleanup(query.Close)
			result := query.Exec(t.Context())
			require.ErrorContains(t, result.Err, tt.want)
		})
	}
}

func TestPinnedRangeFunctionPushdown(t *testing.T) {
	t.Parallel()
	store := promqltest.LoadedStorage(t, twoSeries)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	scanner := &rangeFunctionTestScanner{Scanner: scarecrow.NewQueryableScanner(store)}
	engine := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, NewScanner: func(storage.Queryable) scarecrow.Scanner { return scanner }})
	query, err := engine.NewRangeQuery(t.Context(), store, nil, "rate(http_requests[1m] @ 120 offset 30s)", time.Unix(0, 0), time.Unix(120, 0), time.Minute)
	require.NoError(t, err)
	t.Cleanup(query.Close)
	result := query.Exec(t.Context())
	require.NoError(t, result.Err)
	matrix, err := result.Matrix()
	require.NoError(t, err)
	require.Equal(t, []scarecrow.WindowGrid{{Start: 90_000, Step: 1, NumSteps: 1, Width: 60_000}}, scanner.grids)
	require.Len(t, matrix, 1)
	require.Len(t, matrix[0].Floats, 3)
	for _, point := range matrix[0].Floats {
		require.Equal(t, 2.0, point.F)
	}
}
