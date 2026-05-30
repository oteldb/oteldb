package jsonexpr

import (
	"fmt"
	"slices"

	"github.com/oteldb/oteldb/internal/logql"
)

// SelectorTree is a simple prefix tree of selectors. Efficient for matching multiple JSON paths.
type SelectorTree struct {
	root *selectorNode
}

// IsEmpty returns true if the tree is empty.
func (p SelectorTree) IsEmpty() bool {
	return p.root == nil || len(p.root.key) == 0
}

type selectorNode struct {
	labels []logql.Label
	key    map[string]*selectorNode
	index  map[int]*selectorNode
}

// MakeSelectorTree creates a new SelectorTree from the given paths.
func MakeSelectorTree(paths map[logql.Label]Path) SelectorTree {
	if len(paths) == 0 {
		return SelectorTree{}
	}

	root := &selectorNode{}
	for label, p := range paths {
		n := root
		for i, s := range p {
			var (
				nn *selectorNode
				ok bool
			)
			switch typ := s.Type; typ {
			case Index:
				nn, ok = n.index[s.Index]
				if !ok {
					nn = &selectorNode{}
				}

				if n.index == nil {
					n.index = map[int]*selectorNode{}
				}
				n.index[s.Index] = nn
			case Key:
				nn, ok = n.key[s.Key]
				if !ok {
					nn = &selectorNode{}
				}

				if n.key == nil {
					n.key = map[string]*selectorNode{}
				}
				n.key[s.Key] = nn
			default:
				panic(fmt.Sprintf("unexpected selector type: %v", typ))
			}

			if i == len(p)-1 {
				if !slices.Contains(nn.labels, label) {
					nn.labels = append(nn.labels, label)
				}
			}
			n = nn
		}
	}
	return SelectorTree{
		root: root,
	}
}
