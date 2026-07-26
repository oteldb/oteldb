package scarecrow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnSetClear(t *testing.T) {
	t.Parallel()

	// Spans several bitset words to catch word-boundary indexing.
	const steps = 130

	c := NewColumn(7, steps)
	require.Equal(t, SeriesRef(7), c.Ref)
	require.Equal(t, steps, c.Steps())
	require.True(t, c.Empty())
	require.Zero(t, c.Count())

	for _, i := range []int{0, 63, 64, 65, 127, 129} {
		c.Set(i, float64(i)*1.5)
	}

	require.False(t, c.Empty())
	require.Equal(t, 6, c.Count())

	for i := range steps {
		switch i {
		case 0, 63, 64, 65, 127, 129:
			require.Truef(t, c.IsSet(i), "step %d should be set", i)
			require.InDelta(t, float64(i)*1.5, c.V[i], 0)
		default:
			require.Falsef(t, c.IsSet(i), "step %d should be absent", i)
		}
	}

	c.Clear(64)
	require.False(t, c.IsSet(64))
	require.Equal(t, 5, c.Count())
	// Clearing drops presence, not the stored value: readers must consult the bitset.
	require.InDelta(t, 96.0, c.V[64], 0)
}

func TestColumnResizeReuseIsClean(t *testing.T) {
	t.Parallel()

	c := NewColumn(0, 64)
	for i := range 64 {
		c.Set(i, 1)
	}

	// Shrinking then regrowing within capacity must not resurrect stale bits or values.
	c.Resize(1, 8)
	require.Equal(t, SeriesRef(1), c.Ref)
	require.Equal(t, 8, c.Steps())
	require.True(t, c.Empty())

	c.Resize(2, 64)
	require.True(t, c.Empty(), "regrown column must not expose stale validity")
	for i := range 64 {
		require.InDelta(t, 0.0, c.V[i], 0, "regrown column must not expose stale values")
	}
}

func TestColumnResizeGrows(t *testing.T) {
	t.Parallel()

	c := NewColumn(0, 4)
	c.Resize(3, 200)

	require.Equal(t, 200, c.Steps())
	require.Len(t, c.Valid, wordsFor(200))

	c.Set(199, 42)
	require.True(t, c.IsSet(199))
	require.Equal(t, 1, c.Count())
}

func TestColumnCopyFrom(t *testing.T) {
	t.Parallel()

	src := NewColumn(5, 70)
	src.Set(0, 1)
	src.Set(69, 2)

	dst := NewColumn(0, 3)
	dst.CopyFrom(src)

	require.Equal(t, src.Ref, dst.Ref)
	require.Equal(t, src.Steps(), dst.Steps())
	require.Equal(t, src.Count(), dst.Count())
	require.True(t, dst.IsSet(0))
	require.True(t, dst.IsSet(69))
	require.InDelta(t, 2.0, dst.V[69], 0)

	// The copy must be independent: the concurrency wrapper relies on this.
	src.Set(1, 99)
	require.False(t, dst.IsSet(1))
}
