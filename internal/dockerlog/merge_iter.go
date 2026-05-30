package dockerlog

import (
	"container/heap"

	"go.uber.org/multierr"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

type iterHeapElem struct {
	iterIdx int
	entry   logqlengine.Entry
}

func (a iterHeapElem) Less(b iterHeapElem) bool {
	return a.entry.Timestamp < b.entry.Timestamp
}

type iterHeap []iterHeapElem

func (h iterHeap) Len() int {
	return len(h)
}

func (h iterHeap) Less(i, j int) bool {
	return h[i].Less(h[j])
}

func (h iterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iterHeap) Push(x any) {
	*h = append(*h, x.(iterHeapElem))
}

func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// mergeIter merges several iterators by timestamp.
type mergeIter struct {
	iters       []logqlengine.EntryIterator
	heap        iterHeap
	initialized bool
}

func newMergeIter(iters []logqlengine.EntryIterator) logqlengine.EntryIterator {
	return &mergeIter{
		iters: iters,
	}
}

var _ logqlengine.EntryIterator = (*mergeIter)(nil)

// Next returns true, if there is element and fills t.
func (i *mergeIter) Next(r *logqlengine.Entry) (ok bool) {
	i.init()
	if i.heap.Len() < 1 {
		return false
	}

	// Get min element from heap (record with smallest timestamp).
	e := heap.Pop(&i.heap).(iterHeapElem)
	*r = e.entry

	switch iter := i.iters[e.iterIdx]; {
	case iter.Next(&e.entry):
		// Peek next element from min iterator.
		heap.Push(&i.heap, e)
		return true
	case iter.Err() != nil:
		// Return an error, if read failed.
		return false
	default:
		// heap.Pop removed drained iterator from heap.
		return true
	}
}

func (i *mergeIter) init() {
	if i.initialized {
		return
	}
	i.initialized = true

	// Peek an element from each iterator to
	// find min element.
	var entry logqlengine.Entry
	for idx, iter := range i.iters {
		if !iter.Next(&entry) {
			continue
		}
		heap.Push(&i.heap, iterHeapElem{
			iterIdx: idx,
			entry:   entry,
		})
	}
}

// Err returns an error caused during iteration, if any.
func (i *mergeIter) Err() (rerr error) {
	for _, iter := range i.iters {
		multierr.AppendInto(&rerr, iter.Err())
	}
	return rerr
}

// Close closes iterator.
func (i *mergeIter) Close() (rerr error) {
	for _, iter := range i.iters {
		multierr.AppendInto(&rerr, iter.Close())
	}
	return rerr
}
