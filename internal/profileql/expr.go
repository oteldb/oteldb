package profileql

import (
	"fmt"
	"regexp"
	"strings"
)

// Expr is a parsed ProfileQL query: a profile-type selector together with a set
// of label matchers.
//
// The textual form is a profile-type selector optionally followed by a brace
// enclosed list of label matchers, for example
//
//	process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend"}
//
// mirroring Pyroscope's query syntax, which is a Prometheus metric selector
// whose __name__ is the profile type.
type Expr struct {
	// Type is the parsed profile-type selector.
	Type ProfileType
	// Matchers are the label matchers, excluding the profile-type ([LabelName])
	// matcher.
	Matchers []LabelMatcher
}

// String implements [fmt.Stringer].
func (e Expr) String() string {
	var sb strings.Builder
	sb.WriteString(e.Type.String())
	sb.WriteByte('{')
	for i, m := range e.Matchers {
		if i != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(m.String())
	}
	sb.WriteByte('}')
	return sb.String()
}

// LabelMatcher is a label matching predicate.
type LabelMatcher struct {
	Label Label
	Op    MatchOp        // OpEq, OpNotEq, OpRe, OpNotRe
	Value string         // Equals to value or to the unparsed regexp
	Re    *regexp.Regexp // Equals to nil, if Op is not OpRe or OpNotRe
}

// String implements [fmt.Stringer].
func (m LabelMatcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Label, m.Op, m.Value)
}
