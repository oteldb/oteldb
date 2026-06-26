package profileqlengine

import (
	"bytes"
	"strconv"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

// Collapsed renders the result into the "collapsed" (folded stacks) format:
// one line per stack with a non-zero self value, as a semicolon-separated list
// of frames (root to leaf) followed by a space and the self value.
//
// The synthetic "total" root is omitted from the stacks.
func (r *Result) Collapsed() []byte {
	if r.Tree == nil || r.Tree.Root == nil {
		return nil
	}
	var buf bytes.Buffer
	for _, child := range r.Tree.Root.Children {
		writeCollapsed(&buf, child, "")
	}
	return buf.Bytes()
}

func writeCollapsed(buf *bytes.Buffer, n *profilestorage.FlameNode, prefix string) {
	stack := n.Name
	if prefix != "" {
		stack = prefix + ";" + n.Name
	}
	if n.Self > 0 {
		buf.WriteString(stack)
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatInt(n.Self, 10))
		buf.WriteByte('\n')
	}
	for _, child := range n.Children {
		writeCollapsed(buf, child, stack)
	}
}
