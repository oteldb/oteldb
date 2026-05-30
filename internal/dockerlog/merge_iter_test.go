package dockerlog

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func TestMergeIter(t *testing.T) {
	seriess := [][]otelstorage.Timestamp{
		{1, 5, 6},
		{2, 3, 7},
		{4, 8},
	}

	var (
		iters    = make([]logqlengine.EntryIterator, len(seriess))
		expected []otelstorage.Timestamp
	)
	// Build iterators from given timestamp series.
	for i, series := range seriess {
		elems := make([]logqlengine.Entry, len(series))
		for i, ts := range series {
			elems[i] = logqlengine.Entry{
				Timestamp: ts,
				Line:      fmt.Sprintf("Message #%d", i),
			}
			expected = append(expected, ts)
		}
		iters[i] = iterators.Slice(elems)
	}
	// Expect a sorted list of timestamps.
	slices.Sort(expected)

	var (
		iter   = newMergeIter(iters)
		record logqlengine.Entry
		got    []otelstorage.Timestamp
	)
	for iter.Next(&record) {
		got = append(got, record.Timestamp)
	}
	require.NoError(t, iter.Err())
	require.Equal(t, expected, got)
}
