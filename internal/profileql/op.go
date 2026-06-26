package profileql

import "fmt"

// MatchOp defines a label matching operation.
type MatchOp int

const (
	// OpEq is an equality matcher (=).
	OpEq MatchOp = iota + 1
	// OpNotEq is an inequality matcher (!=).
	OpNotEq
	// OpRe is a regexp matcher (=~).
	OpRe
	// OpNotRe is a negative regexp matcher (!~).
	OpNotRe
)

// String implements [fmt.Stringer].
func (op MatchOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpRe:
		return "=~"
	case OpNotRe:
		return "!~"
	default:
		return fmt.Sprintf("<unknown op %d>", int(op))
	}
}

// IsRegex returns whether the operation is a regexp matcher.
func (op MatchOp) IsRegex() bool {
	switch op {
	case OpRe, OpNotRe:
		return true
	default:
		return false
	}
}
