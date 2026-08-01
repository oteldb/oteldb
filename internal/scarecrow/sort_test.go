package scarecrow

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestAscendingDescendingNaNLast(t *testing.T) {
	nan := math.NaN()

	require.True(t, ascendingNaNLast(1, 2))
	require.False(t, ascendingNaNLast(2, 1))
	require.True(t, ascendingNaNLast(1, nan))
	require.False(t, ascendingNaNLast(nan, 1))
	require.False(t, ascendingNaNLast(nan, nan))

	require.True(t, descendingNaNLast(2, 1))
	require.False(t, descendingNaNLast(1, 2))
	require.True(t, descendingNaNLast(1, nan))
	require.False(t, descendingNaNLast(nan, 1))
	require.False(t, descendingNaNLast(nan, nan))
}

func TestCompareByLabel(t *testing.T) {
	a := labels.FromStrings("instance", "2")
	b := labels.FromStrings("instance", "10")

	// Natural sort: "2" < "10" numerically, unlike a plain string compare.
	require.Negative(t, compareByLabel(a, b, []string{"instance"}, false))
	require.Positive(t, compareByLabel(a, b, []string{"instance"}, true))

	// Equal on every named label: falls back to the full label set for a total order.
	same := labels.FromStrings("instance", "2", "job", "a")
	other := labels.FromStrings("instance", "2", "job", "b")

	require.Negative(t, compareByLabel(same, other, []string{"instance"}, false))
}
