package scarecrow_test

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// loadScanner builds a Scanner over a promqltest-loaded storage holding [twoSeries].
func loadScanner(t *testing.T) scarecrow.Scanner {
	t.Helper()

	st := promqltest.LoadedStorage(t, twoSeries)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	return scarecrow.NewQueryableScanner(st)
}

const twoSeries = `
load 30s
  http_requests{job="api", code="200"} 0+10x5
  http_requests{job="api", code="500"} 0+1x5
`

func TestQueryableScannerSeries(t *testing.T) {
	t.Parallel()

	sc := loadScanner(t)
	ctx := context.Background()

	m := mustMatchers(t, `http_requests`)

	got, err := sc.Series(ctx, 0, 300_000, m)
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Sorted select means schema order is deterministic run to run.
	require.Equal(t, `{__name__="http_requests", code="200", job="api"}`, got[0].String())
	require.Equal(t, `{__name__="http_requests", code="500", job="api"}`, got[1].String())
}

func TestQueryableScannerScan(t *testing.T) {
	t.Parallel()

	sc := loadScanner(t)
	ctx := context.Background()

	it, err := sc.Scan(ctx, 0, 300_000, mustMatchers(t, `http_requests{code="200"}`))
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	s, err := it.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, s)

	require.Equal(t, `{__name__="http_requests", code="200", job="api"}`, s.Labels.String())
	require.Equal(t, []int64{0, 30_000, 60_000, 90_000, 120_000, 150_000}, s.T)
	require.Equal(t, []float64{0, 10, 20, 30, 40, 50}, s.V)

	// Unsampled data carries no weights, and Weight defaults to 1.
	require.Nil(t, s.Weights)
	require.InDelta(t, 1.0, s.Weight(0), 0)

	next, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, next, "only one series matches")
}

func TestQueryableScannerWindowClamps(t *testing.T) {
	t.Parallel()

	sc := loadScanner(t)
	ctx := context.Background()

	it, err := sc.Scan(ctx, 60_000, 90_000, mustMatchers(t, `http_requests{code="200"}`))
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	s, err := it.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, []int64{60_000, 90_000}, s.T)
	require.Equal(t, []float64{20, 30}, s.V)
}

func TestQueryableScannerSkipsEmptySeries(t *testing.T) {
	t.Parallel()

	sc := loadScanner(t)
	ctx := context.Background()

	// A window past every sample must yield nothing, not an empty series: PromQL drops those.
	it, err := sc.Scan(ctx, 1_000_000, 2_000_000, mustMatchers(t, `http_requests`))
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	s, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, s)
}

func TestQueryableScannerNoMatch(t *testing.T) {
	t.Parallel()

	sc := loadScanner(t)
	ctx := context.Background()

	got, err := sc.Series(context.Background(), 0, 300_000, mustMatchers(t, `nonexistent`))
	require.NoError(t, err)
	require.Empty(t, got)

	it, err := sc.Scan(ctx, 0, 300_000, mustMatchers(t, `nonexistent`))
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	s, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, s)
}
