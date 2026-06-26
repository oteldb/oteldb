package profilestorage

// FlameTree is a merged, symbol-resolved flamegraph tree.
//
// It is the storage-agnostic representation produced by
// [Querier.SelectMergeProfile] and consumed by the ProfileQL engine, which
// converts it into the API-specific flamebearer format.
type FlameTree struct {
	// Root is the root node of the tree. It may be nil for an empty result.
	Root *FlameNode
}

// FlameNode is a single node of a [FlameTree].
type FlameNode struct {
	// Name is the resolved function name of the node.
	Name string
	// Self is the value of samples attributed directly to this node.
	Self int64
	// Total is the value of samples attributed to this node and its children.
	Total int64
	// Children are the child nodes, ordered left to right.
	Children []*FlameNode
}

// Total returns the total value of the tree, or zero if it is empty.
func (t *FlameTree) Total() int64 {
	if t == nil || t.Root == nil {
		return 0
	}
	return t.Root.Total
}
