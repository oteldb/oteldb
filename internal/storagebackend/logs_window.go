package storagebackend

import (
	"maps"
	"math"
	"slices"
)

type logWindowChange struct {
	// Empty bodies still make a group present in a window.
	rows, bytes int64
}

type logWindowGroup struct {
	steps   int
	changes []logWindowChange
	sparse  map[int]logWindowChange
	total   int64
	direct  map[int]float64
}

func newLogWindowGroup(steps int, denseBudget *int64) *logWindowGroup {
	g := &logWindowGroup{steps: steps}
	if int64(steps) < *denseBudget {
		g.changes = make([]logWindowChange, steps+1)
		*denseBudget -= int64(steps) + 1
	} else {
		g.sparse = map[int]logWindowChange{}
	}
	return g
}

func (g *logWindowGroup) change(step int, rows, bytes int64) {
	if g.changes != nil {
		g.changes[step].rows += rows
		g.changes[step].bytes += bytes
		return
	}
	change := g.sparse[step]
	change.rows += rows
	change.bytes += bytes
	g.sparse[step] = change
}

func (g *logWindowGroup) add(first, last int, bytes int64) {
	if g.direct == nil && bytes <= math.MaxInt64-g.total {
		g.total += bytes
		g.change(first, 1, bytes)
		g.change(last+1, -1, -bytes)
		return
	}
	g.change(first, 1, 0)
	g.change(last+1, -1, 0)
	if g.direct == nil && bytes > math.MaxInt64-g.total {
		// Preserve floating aggregation beyond the integer accumulator's range.
		g.direct = map[int]float64{}
		_ = g.visit(func(first, end int, _, total int64) error {
			for step := first; step < end; step++ {
				g.direct[step] = float64(total)
			}
			return nil
		})
	}
	for step := first; step <= last; step++ {
		g.direct[step] += float64(bytes)
	}
}

func (g *logWindowGroup) visit(emit func(first, end int, rows, bytes int64) error) error {
	var rows, bytes int64
	if g.changes != nil {
		for step := range g.steps {
			rows += g.changes[step].rows
			bytes += g.changes[step].bytes
			if rows > 0 {
				if err := emit(step, step+1, rows, bytes); err != nil {
					return err
				}
			}
		}
		return nil
	}
	keys := slices.Sorted(maps.Keys(g.sparse))
	for i, step := range keys {
		rows += g.sparse[step].rows
		bytes += g.sparse[step].bytes
		if rows <= 0 || step >= g.steps {
			continue
		}
		end := g.steps
		if i+1 < len(keys) {
			end = min(keys[i+1], end)
		}
		if err := emit(step, end, rows, bytes); err != nil {
			return err
		}
	}
	return nil
}
