package storagebackend

import (
	"math"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogWindowGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		updates [][3]int64
	}{
		{"empty", nil},
		{"empty bodies", [][3]int64{{0, 4, 0}, {2, 2, 0}}},
		{"overlap", [][3]int64{{0, 4, 2}, {1, 3, 3}, {2, 2, 7}}},
		{"floating fallback", [][3]int64{{0, 1, math.MaxInt64}, {1, 2, 1}}},
	} {
		for _, dense := range []bool{false, true} {
			t.Run(tc.name+"/dense="+strconv.FormatBool(dense), func(t *testing.T) {
				t.Parallel()
				var budget int64
				if dense {
					budget = 6
				}
				group := newLogWindowGroup(5, &budget)
				var wantRows [5]int64
				var wantBytes [5]float64
				for _, update := range tc.updates {
					first, last, bytes := update[0], update[1], update[2]
					group.add(int(first), int(last), bytes)
					for step := first; step <= last; step++ {
						wantRows[step]++
						wantBytes[step] += float64(bytes)
					}
				}
				rows, bytes := collectLogWindows(t, group)
				require.Equal(t, wantRows[:], rows)
				require.Equal(t, wantBytes[:], bytes)
			})
		}
	}
}

func TestLogWindowGroupMatchesFanout(t *testing.T) {
	t.Parallel()
	const steps = 37
	rng := rand.New(rand.NewPCG(31, 41))
	var budget int64
	group := newLogWindowGroup(steps, &budget)
	var wantRows, wantBytes [steps]int64
	for range 1000 {
		first := rng.IntN(steps)
		last := first + rng.IntN(steps-first)
		bytes := int64(rng.IntN(1000))
		group.add(first, last, bytes)
		for step := first; step <= last; step++ {
			wantRows[step]++
			wantBytes[step] += bytes
		}
	}
	rows, bytes := collectLogWindows(t, group)
	for step := range steps {
		require.Equal(t, wantRows[step], rows[step])
		require.Equal(t, float64(wantBytes[step]), bytes[step])
	}
}

func collectLogWindows(t *testing.T, group *logWindowGroup) (rows []int64, bytes []float64) {
	t.Helper()
	rows = make([]int64, group.steps)
	bytes = make([]float64, group.steps)
	require.NoError(t, group.visit(func(first, end int, count, total int64) error {
		for step := first; step < end; step++ {
			rows[step] = count
			bytes[step] = float64(total)
			if group.direct != nil {
				bytes[step] = group.direct[step]
			}
		}
		return nil
	}))
	return rows, bytes
}

func TestLogWindowGroupsBoundSparseState(t *testing.T) {
	t.Parallel()
	const groups, steps = 1000, 10_000
	budget := int64(groups)
	var denseCells, sparseEvents, emitted int
	for i := range groups {
		group := newLogWindowGroup(steps, &budget)
		group.add(i, i+1, int64(i))
		denseCells += len(group.changes)
		sparseEvents += len(group.sparse)
		require.NoError(t, group.visit(func(first, end int, rows, bytes int64) error {
			emitted += end - first
			require.Equal(t, int64(1), rows)
			require.Equal(t, int64(i), bytes)
			return nil
		}))
	}
	require.LessOrEqual(t, denseCells, groups)
	require.Equal(t, groups*2, sparseEvents)
	require.Equal(t, groups*2, emitted)
}

func TestLogWindowDenseBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		budget int64
		dense  bool
	}{
		{"too small", 5, false},
		{"exact", 6, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			budget := tc.budget
			group := newLogWindowGroup(5, &budget)
			require.Equal(t, tc.dense, group.changes != nil)
			require.Equal(t, tc.budget-int64(len(group.changes)), budget)
			require.GreaterOrEqual(t, budget, int64(0))
		})
	}
}
