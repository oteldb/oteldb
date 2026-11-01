package scarecrow

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

type nativeCountFixture struct {
	*projectedCountScanner
	counts     map[string][]uint64
	err        error
	countCalls int
}

func (f *nativeCountFixture) CountSeriesGrid(context.Context, WindowGrid, string, []*labels.Matcher) (map[string][]uint64, error) {
	f.countCalls++
	return f.counts, f.err
}

func (*nativeCountFixture) CountSeries(context.Context, int64, int64, []*labels.Matcher) (uint64, error) {
	panic("grid used a per-window count")
}

func TestNativeCountGridFires(t *testing.T) {
	t.Parallel()
	base, ec := projectionFixture()
	f := &nativeCountFixture{projectedCountScanner: base, counts: map[string][]uint64{
		"": {0, 1, 0, 0, 1, 0}, "busy": {0, 0, 1, 2, 1, 2}, "idle": {0, 2, 1, 0, 1, 2},
	}}
	op := newCountSeriesBy(f, f, nil, "", "mode", 0, nil, ec)
	schema, err := op.Schema(t.Context())
	require.NoError(t, err)
	require.Len(t, schema.Series, 3)
	for _, expected := range [][]float64{{0, 1, 0, 0, 1, 0}, {0, 0, 1, 2, 1, 2}, {0, 2, 1, 0, 1, 2}} {
		col, err := op.Next(t.Context())
		require.NoError(t, err)
		require.Equal(t, expected, col.V)
		for step, n := range expected {
			require.Equal(t, n > 0, col.IsSet(step))
		}
	}
	require.Equal(t, 1, f.countCalls)
	require.Zero(t, f.calls, "full aggregates are bypassed")
	base, ec = projectionFixture()
	f = &nativeCountFixture{projectedCountScanner: base, counts: map[string][]uint64{"": {0, 3, 2, 2, 3, 4}}}
	ungrouped := newCountSeries(f, f, nil, "", 0, nil, ec)
	col, err := ungrouped.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []float64{0, 3, 2, 2, 3, 4}, col.V)
	require.Equal(t, 1, f.countCalls)
	require.Zero(t, f.calls)
}

func TestNativeCountGridFallbackAndErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("failed count")
	for _, grouped := range []bool{false, true} {
		for _, failure := range []error{ErrCountGridUnsupported, boom} {
			base, ec := projectionFixture()
			f := &nativeCountFixture{projectedCountScanner: base, err: failure}
			var err error
			if grouped {
				_, err = newCountSeriesBy(f, f, nil, "", "mode", 0, nil, ec).Next(t.Context())
			} else {
				_, err = newCountSeries(f, f, nil, "", 0, nil, ec).Next(t.Context())
			}
			if failure == ErrCountGridUnsupported {
				require.NoError(t, err)
				require.Equal(t, 1, f.calls)
			} else {
				require.ErrorIs(t, err, boom)
				require.Zero(t, f.calls, "real failures are not hidden by fallback")
			}
			require.Equal(t, 1, f.countCalls)
		}
	}
}
