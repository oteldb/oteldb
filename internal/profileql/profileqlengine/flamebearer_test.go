package profileqlengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

func TestBuildFlamebearer(t *testing.T) {
	// root(total=10,self=0)
	//   a(total=6,self=2)
	//     a1(total=4,self=4)
	//   b(total=4,self=4)
	tree := &profilestorage.FlameTree{
		Root: &profilestorage.FlameNode{
			Name: "root", Total: 10, Self: 0,
			Children: []*profilestorage.FlameNode{
				{
					Name: "a", Total: 6, Self: 2,
					Children: []*profilestorage.FlameNode{
						{Name: "a1", Total: 4, Self: 4},
					},
				},
				{Name: "b", Total: 4, Self: 4},
			},
		},
	}

	fb := buildFlamebearer(tree, 0)

	require.Equal(t, []string{"total", "a", "a1", "b"}, fb.Names)
	require.Equal(t, [][]int{
		{0, 10, 0, 0},
		{0, 6, 2, 1, 0, 4, 4, 3},
		{2, 4, 4, 2},
	}, fb.Levels)
	require.Equal(t, 10, fb.NumTicks)
	require.Equal(t, 4, fb.MaxSelf)
}

func TestBuildFlamebearerEmpty(t *testing.T) {
	for _, tree := range []*profilestorage.FlameTree{
		nil,
		{},
		{Root: nil},
	} {
		fb := buildFlamebearer(tree, 0)
		// Names/Levels must be non-nil for JSON encoding.
		require.NotNil(t, fb.Names)
		require.NotNil(t, fb.Levels)
		require.Empty(t, fb.Names)
		require.Empty(t, fb.Levels)
		require.Equal(t, 0, fb.NumTicks)
	}
}

func TestBuildFlamebearerMaxNodes(t *testing.T) {
	// root has three children; with maxNodes=1 the two smallest are folded
	// into an "other" node.
	tree := &profilestorage.FlameTree{
		Root: &profilestorage.FlameNode{
			Name: "root", Total: 60, Self: 0,
			Children: []*profilestorage.FlameNode{
				{Name: "big", Total: 30, Self: 30},
				{Name: "mid", Total: 20, Self: 20},
				{Name: "small", Total: 10, Self: 10},
			},
		},
	}

	fb := buildFlamebearer(tree, 1)
	require.Contains(t, fb.Names, "big")
	require.Contains(t, fb.Names, "other")
	require.NotContains(t, fb.Names, "small")
	require.NotContains(t, fb.Names, "mid")

	// The "other" node aggregates mid+small.
	otherIdx := -1
	for i, n := range fb.Names {
		if n == "other" {
			otherIdx = i
		}
	}
	require.NotEqual(t, -1, otherIdx)

	var otherTotal int
	for _, l := range fb.Levels {
		for i := 0; i < len(l); i += 4 {
			if l[i+3] == otherIdx {
				otherTotal = l[i+1]
			}
		}
	}
	require.Equal(t, 30, otherTotal)
}

func TestSampleRate(t *testing.T) {
	require.Equal(t, uint32(1_000_000_000), sampleRate(mustType(t, "process_cpu:cpu:nanoseconds:cpu:nanoseconds")))
	require.Equal(t, uint32(100), sampleRate(mustType(t, "memory:alloc_space:bytes:space:bytes")))
}
