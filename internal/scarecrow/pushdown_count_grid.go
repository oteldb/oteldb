package scarecrow

import (
	"cmp"
	"context"
	"slices"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
)

func (o *countSeriesBy) loadGrid(ctx context.Context, grid WindowGrid) error {
	if counter, ok := o.grid.(GridSeriesCounter); ok {
		counts, err := counter.CountSeriesGrid(ctx, grid, o.by, o.matchers)
		if !errors.Is(err, ErrCountGridUnsupported) {
			if err != nil {
				return err
			}
			return o.setGridCounts(ctx, grid, counts)
		}
	}
	series, err := aggregateGrid(ctx, o.ec, o.grid, grid, o.matchers)
	if err != nil {
		return err
	}
	groupOf := make([]int, len(series))
	groups := make([]string, 0)
	byValue := make(map[string]int)
	for i, s := range series {
		groupOf[i] = -1
		if !slices.ContainsFunc(s.Windows, func(w Aggregate) bool { return w.Rows > 0 }) {
			continue
		}
		value := s.Labels.Get(o.by)
		group, ok := byValue[value]
		if !ok {
			group = len(groups)
			groups = append(groups, value)
			byValue[value] = group
		}
		groupOf[i] = group
	}
	if err := o.ec.charge(len(groups) * grid.NumSteps); err != nil {
		return err
	}
	order := make([]int, len(groups))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(groups[a], groups[b]) })
	remap := make([]int, len(groups))
	projected := make([]labels.Labels, len(groups))
	for ref, group := range order {
		remap[group] = ref
		if groups[group] == "" {
			projected[ref] = labels.EmptyLabels()
		} else {
			projected[ref] = labels.FromStrings(o.by, groups[group])
		}
	}
	o.schema = NewSchema(projected)
	o.values = make([]float64, len(groups)*grid.NumSteps)
	o.valid = make([]bool, len(o.values))
	for i, s := range series {
		if groupOf[i] < 0 {
			continue
		}
		base := remap[groupOf[i]] * grid.NumSteps
		for step, w := range s.Windows {
			if w.Rows > 0 {
				o.values[base+step]++
				o.valid[base+step] = true
			}
		}
	}
	return nil
}
