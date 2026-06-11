package traceqlengine

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tempoapi"

	"github.com/stretchr/testify/require"
)

func TestEngine(t *testing.T) {
	tests := []struct {
		query       string
		left, right []spanIDs
		wantResult  []uint64
		wantErr     bool
	}{
		// Span matching.
		{
			`{ name = "Span #1" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			nil,
			[]uint64{
				1,
			},
			false,
		},
		{
			`{ rootServiceName = "test.service" }`,
			[]spanIDs{
				{id: 2, parent: 1},
				{id: 3, parent: 2},
				{id: 1},
			},
			nil,
			[]uint64{
				1, 2, 3,
			},
			false,
		},
		{
			`{ rootName = "Span #1" }`,
			[]spanIDs{
				{id: 2, parent: 1},
				{id: 3, parent: 2},
				{id: 1},
			},
			nil,
			[]uint64{
				1, 2, 3,
			},
			false,
		},
		{
			`{ name = "Span #1" || name = "Span #3" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			nil,
			[]uint64{
				1, 3,
			},
			false,
		},
		{
			`{ name =~ "^Span #\\d$" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
				{id: 10},
			},
			nil,
			[]uint64{
				1, 2, 3,
			},
			false,
		},
		{
			`{ .group = "left" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			nil,
			[]uint64{
				1, 2, 3,
			},
			false,
		},
		// Binary spanset expression.
		{
			`{ .group = "left" } && { .group = "right" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			[]spanIDs{
				{id: 4},
				{id: 5},
				{id: 6},
			},
			[]uint64{
				1, 2, 3,
				4, 5, 6,
			},
			false,
		},
		{
			`{ .group = "no_match" } && { .group = "right" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			[]spanIDs{
				{id: 4},
				{id: 5},
				{id: 6},
			},
			nil,
			false,
		},
		{
			`{ .group = "left" } || { .group = "right" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			[]spanIDs{
				{id: 4},
				{id: 5},
				{id: 6},
			},
			[]uint64{
				1, 2, 3,
				4, 5, 6,
			},
			false,
		},
		{
			`{ .group = "no_match" } || { .group = "right" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			[]spanIDs{
				{id: 4},
				{id: 5},
				{id: 6},
			},
			[]uint64{
				4, 5, 6,
			},
			false,
		},
		{
			`{ .group = "left" } > { .group = "right" }`,
			[]spanIDs{
				{id: 1},
				{id: 2},
				{id: 3},
			},
			[]spanIDs{
				{id: 1},
				{id: 4, parent: 2},
				{id: 5, parent: 3},
				{id: 6, parent: 3},
				{id: 7, parent: 4},
			},
			[]uint64{
				4, 5, 6,
			},
			false,
		},
		// Binary expression.
		{
			`({ .group = "left" } | count() > 0) ~ ({ .group = "right" } | count() > 0)`,
			[]spanIDs{
				{id: 1},
				{id: 2, parent: 12},
				{id: 3, parent: 13},
			},
			[]spanIDs{
				{id: 4},
				{id: 5, parent: 12},
				{id: 6, parent: 13},
				{id: 7, parent: 1},
				{id: 8, parent: 14},
			},
			[]uint64{
				4, 5, 6,
			},
			false,
		},

		// Invalid query
		{
			`{ .a = }`,
			nil,
			nil,
			nil,
			true,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			q := MemoryQuerier{}
			for _, span := range generateSpans(tt.left, "left") {
				q.Add(span)
			}
			for _, span := range generateSpans(tt.right, "right") {
				q.Add(span)
			}
			engine := NewEngine(&q, Options{})

			result, err := engine.Eval(context.Background(), tt.query, EvalParams{Limit: 100})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tt.wantResult, getResultSpanIDs(t, result))
		})
	}
}

func getResultSpanIDs(t require.TestingT, result *tempoapi.Traces) (got []uint64) {
	for _, trace := range result.Traces {
		for _, span := range trace.SpanSet.Value.Spans {
			id, err := hex.DecodeString(span.SpanID)
			require.NoError(t, err)
			require.Len(t, id, 8)

			got = append(got, otelstorage.SpanID(id).AsUint64())
		}
	}
	slices.Sort(got)
	return got
}

func TestTimeRange(t *testing.T) {
	tests := []struct {
		trange     timeRange
		start, end otelstorage.Timestamp
		want       bool
	}{
		{
			timeRange{}, // no restrictions
			10, 12,
			true,
		},
		// Only start.
		{
			timeRange{start: time.Unix(0, 9)},
			10, 12,
			true,
		},
		{
			timeRange{start: time.Unix(0, 11)},
			10, 12,
			false,
		},
		// Only end.
		{
			timeRange{end: time.Unix(0, 13)},
			10, 12,
			true,
		},
		{
			timeRange{end: time.Unix(0, 11)},
			10, 12,
			false,
		},
		// Both.
		{
			timeRange{start: time.Unix(0, 9), end: time.Unix(0, 13)},
			10, 12,
			true,
		},
		{
			timeRange{start: time.Unix(0, 9), end: time.Unix(0, 1)},
			5, 15,
			false,
		},
		// Min duration.
		{
			timeRange{min: 1},
			10, 12,
			true,
		},
		{
			timeRange{min: 5},
			10, 12,
			false,
		},
		// Max duration.
		{
			timeRange{max: 5},
			10, 12,
			true,
		},
		{
			timeRange{max: 1},
			10, 12,
			false,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, tt.trange.within(
				tt.start.AsTime(),
				tt.end.AsTime(),
			))
		})
	}
}
