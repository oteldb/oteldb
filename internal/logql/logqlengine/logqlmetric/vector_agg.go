package logqlmetric

import (
	"container/heap"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

// VectorAggregation creates new Vector aggregation step iterator.
func VectorAggregation(
	iter iterators.Iterator[Step],
	expr *logql.VectorAggregationExpr,
) (StepIterator, error) {
	var (
		grouper = func(logqlabels.AggregatedLabels, ...logql.Label) logqlabels.AggregatedLabels {
			return logqlabels.EmptyAggregatedLabels()
		}
		groupLabels []logql.Label
	)
	if g := expr.Grouping; g != nil {
		groupLabels = g.Labels
		if g.Without {
			grouper = logqlabels.AggregatedLabels.Without
		} else {
			grouper = logqlabels.AggregatedLabels.By
		}
	}

	param := -1
	if p := expr.Parameter; p != nil {
		param = *p
	}
	switch expr.Op {
	case logql.VectorOpBottomk, logql.VectorOpSort:
		return &vectorAggHeapIterator{
			iter:        iter,
			limit:       param,
			less:        Sample.Less,
			greater:     Sample.Greater,
			grouper:     grouper,
			groupLabels: groupLabels,
		}, nil
	case logql.VectorOpTopk, logql.VectorOpSortDesc:
		return &vectorAggHeapIterator{
			iter:        iter,
			limit:       param,
			less:        Sample.Greater,
			greater:     Sample.Less,
			grouper:     grouper,
			groupLabels: groupLabels,
		}, nil
	}

	agg, err := buildAggregator(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build aggregator")
	}

	return &vectorAggIterator{
		iter:        iter,
		agg:         agg,
		grouper:     grouper,
		groupLabels: groupLabels,
	}, nil
}

type vectorAggIterator struct {
	iter        iterators.Iterator[Step]
	agg         func() Aggregator
	grouper     grouperFunc
	groupLabels []logql.Label
}

func (i *vectorAggIterator) Next(r *Step) bool {
	var step Step
	if !i.iter.Next(&step) {
		return false
	}

	type group struct {
		metric logqlabels.AggregatedLabels
		agg    Aggregator
	}
	result := map[logqlabels.GroupingKey]*group{}

	for _, s := range step.Samples {
		metric := i.grouper(s.Set, i.groupLabels...)
		groupKey := metric.Key()

		g, ok := result[groupKey]
		if !ok {
			g = &group{
				metric: metric,
				agg:    i.agg(),
			}
			result[groupKey] = g
		}
		g.agg.Apply(s.Data)
	}

	r.Timestamp = step.Timestamp
	r.Samples = r.Samples[:0]
	for _, g := range result {
		r.Samples = append(r.Samples, Sample{
			Data: g.agg.Result(),
			Set:  g.metric,
		})
	}

	return true
}

func (i *vectorAggIterator) Err() error {
	return i.iter.Err()
}

func (i *vectorAggIterator) Close() error {
	return i.iter.Close()
}

type vectorAggHeapIterator struct {
	iter    iterators.Iterator[Step]
	limit   int
	less    func(a, b Sample) bool
	greater func(a, b Sample) bool

	grouper     grouperFunc
	groupLabels []logql.Label
}

func (i *vectorAggHeapIterator) Next(r *Step) bool {
	var step Step
	if !i.iter.Next(&step) {
		return false
	}
	r.Timestamp = step.Timestamp
	if i.limit == 0 {
		// Limit is 0, do nothing.
		return true
	}

	type group struct {
		metric logqlabels.AggregatedLabels
		heap   *sampleHeap
	}
	result := map[logqlabels.GroupingKey]*group{}

	for _, s := range step.Samples {
		metric := i.grouper(s.Set, i.groupLabels...)
		groupKey := metric.Key()

		g, ok := result[groupKey]
		if !ok {
			g = &group{
				metric: metric,
				// To do ascending sorting we need a max heap: find out the biggest
				// value in a heap of smallest values to compare with new sample.
				heap: &sampleHeap{compare: i.greater},
			}

			result[groupKey] = g
		}

		switch {
		case i.limit < 0:
			// Sorting, just append.
			g.heap.elements = append(g.heap.elements, s)
		case g.heap.Len() < i.limit:
			// Heap is not full, just push.
			heap.Push(g.heap, s)
		case i.less(s, g.heap.Min()):
			// Heap is full, but new element is smaller than biggest in heap, so remove the old and add new.
			heap.Pop(g.heap)
			heap.Push(g.heap, s)
		}
	}

	r.Samples = r.Samples[:0]
	for _, g := range result {
		samples := g.heap.elements
		slices.SortFunc(samples, func(a, b Sample) int {
			if i.less(a, b) {
				return -1
			}
			return 0
		})
		r.Samples = append(r.Samples, samples...)
	}

	return true
}

func (i *vectorAggHeapIterator) Err() error {
	return i.iter.Err()
}

func (i *vectorAggHeapIterator) Close() error {
	return i.iter.Close()
}

type sampleHeap struct {
	elements []Sample
	compare  func(a, b Sample) bool
}

func (h *sampleHeap) Min() Sample {
	return h.elements[0]
}

func (h *sampleHeap) Len() int {
	return len(h.elements)
}

func (h *sampleHeap) Less(i, j int) bool {
	return h.compare(h.elements[i], h.elements[j])
}

func (h *sampleHeap) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func (h *sampleHeap) Push(x any) {
	h.elements = append(h.elements, x.(Sample))
}

func (h *sampleHeap) Pop() any {
	idx := len(h.elements) - 1
	e := h.elements[idx]
	h.elements = h.elements[:idx]
	return e
}
