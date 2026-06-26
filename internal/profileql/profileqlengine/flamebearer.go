package profileqlengine

import (
	"sort"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

// buildFlamebearerProfile converts a merged flamegraph tree into a Pyroscope
// flamebearer profile.
func buildFlamebearerProfile(tree *profilestorage.FlameTree, typ profileql.ProfileType, maxNodes int) *pyroscopeapi.FlamebearerProfileV1 {
	flamebearer := buildFlamebearer(tree, maxNodes)

	units := profilestorage.UnitsForProfileType(typ)
	metadata := pyroscopeapi.FlamebearerMetadataV1{
		Name:       pyroscopeapi.NewOptString(typ.SampleType),
		SpyName:    pyroscopeapi.NewOptString("oteldb"),
		SampleRate: pyroscopeapi.NewOptUint32(sampleRate(typ)),
		Units:      pyroscopeapi.NewOptFlamebearerMetadataV1Units(pyroscopeapi.FlamebearerMetadataV1Units(units)),
		Format:     "single",
	}

	return &pyroscopeapi.FlamebearerProfileV1{
		Flamebearer: flamebearer,
		Metadata:    metadata,
	}
}

// buildFlamebearer serializes the flamegraph tree into the flamebearer "single"
// format: per level, chunks of four ints [xOffset (delta-encoded), total, self,
// nameIndex]. Names[0] is always the synthetic "total" root.
func buildFlamebearer(tree *profilestorage.FlameTree, maxNodes int) pyroscopeapi.FlamebearerV1 {
	fb := pyroscopeapi.FlamebearerV1{
		Names:  []string{},
		Levels: [][]int{},
	}
	if tree == nil || tree.Root == nil {
		return fb
	}

	b := flamebearerBuilder{
		names:     fb.Names,
		nameIndex: map[string]int{},
		minVal:    minValue(tree.Root, maxNodes),
	}
	b.walk(tree.Root, 0, 0)
	b.deltaEncode()

	fb.Names = b.names
	fb.Levels = b.levels
	fb.NumTicks = int(tree.Root.Total)
	fb.MaxSelf = int(b.maxSelf)
	return fb
}

type flamebearerBuilder struct {
	names     []string
	nameIndex map[string]int
	levels    [][]int
	maxSelf   int64
	// minVal is the threshold below which sibling nodes are folded into an
	// "other" node; zero disables folding.
	minVal int64
}

func (b *flamebearerBuilder) intern(name string) int {
	if i, ok := b.nameIndex[name]; ok {
		return i
	}
	i := len(b.names)
	if i == 0 {
		// The root node is always rendered as "total".
		name = "total"
	}
	b.nameIndex[name] = i
	b.names = append(b.names, name)
	return i
}

func (b *flamebearerBuilder) walk(n *profilestorage.FlameNode, level int, xOffset int64) {
	if n.Self > b.maxSelf {
		b.maxSelf = n.Self
	}
	idx := b.intern(n.Name)
	if level == len(b.levels) {
		b.levels = append(b.levels, nil)
	}
	b.levels[level] = append(b.levels[level], int(xOffset), int(n.Total), int(n.Self), idx)

	childX := xOffset + n.Self
	var otherTotal int64
	for _, c := range n.Children {
		if b.minVal > 0 && c.Total < b.minVal {
			otherTotal += c.Total
			continue
		}
		b.walk(c, level+1, childX)
		childX += c.Total
	}
	if otherTotal > 0 {
		b.walk(&profilestorage.FlameNode{
			Name:  "other",
			Self:  otherTotal,
			Total: otherTotal,
		}, level+1, childX)
	}
}

// deltaEncode replaces the absolute x offsets (index 0 of each chunk) with
// deltas relative to the end of the previous node on the same level.
func (b *flamebearerBuilder) deltaEncode() {
	for _, l := range b.levels {
		prev := 0
		for i := 0; i < len(l); i += 4 {
			abs := l[i]
			l[i] = abs - prev
			prev = abs + l[i+1]
		}
	}
}

// minValue returns the smallest node total that should be kept so that at most
// maxNodes nodes are rendered; nodes with a smaller total are folded into an
// "other" node. It returns zero (no folding) when maxNodes is not positive or
// the tree already fits.
func minValue(root *profilestorage.FlameNode, maxNodes int) int64 {
	if maxNodes <= 0 {
		return 0
	}
	var totals []int64
	var collect func(n *profilestorage.FlameNode)
	collect = func(n *profilestorage.FlameNode) {
		for _, c := range n.Children {
			totals = append(totals, c.Total)
			collect(c)
		}
	}
	collect(root)
	if len(totals) <= maxNodes {
		return 0
	}
	sort.Slice(totals, func(i, j int) bool {
		return totals[i] > totals[j]
	})
	return totals[maxNodes-1]
}

// sampleRate returns the conventional sample rate for a profile type, following
// Pyroscope's behavior.
func sampleRate(t profileql.ProfileType) uint32 {
	if t.SampleType == "cpu" {
		return 1_000_000_000
	}
	return 100
}
