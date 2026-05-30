package logql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlpattern"
)

// PipelineStage is a LogQL pipeline stage.
type PipelineStage interface {
	pipelineStage()
}

func (*LineFilter) pipelineStage()             {}
func (*JSONExpressionParser) pipelineStage()   {}
func (*LogfmtExpressionParser) pipelineStage() {}
func (*RegexpLabelParser) pipelineStage()      {}
func (*PatternLabelParser) pipelineStage()     {}
func (*UnpackLabelParser) pipelineStage()      {}
func (*LineFormat) pipelineStage()             {}
func (*DecolorizeExpr) pipelineStage()         {}
func (*LabelFilter) pipelineStage()            {}
func (*LabelFormatExpr) pipelineStage()        {}
func (*DropLabelsExpr) pipelineStage()         {}
func (*KeepLabelsExpr) pipelineStage()         {}
func (*DistinctFilter) pipelineStage()         {}

// LineFilter is a line filter (`|=`, `!=`, `=~`, `!~`, `|>`, `!>`).
type LineFilter struct {
	Op BinOp // OpEq, OpNotEq, OpRe, OpNotRe, OpPattern, OpNotPattern
	By LineFilterValue
	Or []LineFilterValue
}

// String implements [fmt.Stringer].
func (f LineFilter) String() string {
	var (
		sb  strings.Builder
		buf = make([]byte, 0, 32)
	)
	switch f.Op {
	case OpEq:
		sb.WriteString("|=")
	case OpNotEq:
		sb.WriteString("!=")
	case OpRe:
		sb.WriteString("|~")
	case OpNotRe:
		sb.WriteString("!~")
	case OpPattern:
		sb.WriteString("|>")
	case OpNotPattern:
		sb.WriteString("!>")
	default:
		sb.WriteString("<invalid op:")
		sb.WriteString(f.Op.String())
		sb.WriteByte('>')
	}
	sb.WriteByte(' ')

	f.By.write(&sb, &buf)
	for i := range f.Or {
		sb.WriteString(" or ")
		f.Or[i].write(&sb, &buf)
	}
	return sb.String()
}

// LineFilterValue is a line filter literal to search by.
type LineFilterValue struct {
	Value string         // Equals to value or to unparsed regexp
	Re    *regexp.Regexp // Equals to nil, if Op is not OpRe or OpNotRe
	IP    bool           // true, if this line filter is IP filter.
}

func (v LineFilterValue) write(sb *strings.Builder, buf *[]byte) {
	if v.IP {
		sb.WriteString("ip(")
	}
	quoted := strconv.AppendQuote((*buf)[:0], v.Value)
	sb.Write(quoted)
	if v.IP {
		sb.WriteByte(')')
	}
}

// JSONExpressionParser extracts and filters labels from JSON.
type JSONExpressionParser struct {
	// Labels is a set of labels to extract.
	Labels []Label
	// Exprs is a set of extraction expressions.
	Exprs []LabelExtractionExpr
}

// LogfmtExpressionParser extracts and filters labels from Logfmt.
type LogfmtExpressionParser struct {
	// Labels is a set of labels to extract.
	Labels []Label
	// Exprs is a set of extraction expressions.
	Exprs []LabelExtractionExpr
	// Flags defines parser flags.
	Flags LogfmtFlags
}

// LogfmtFlags defines logfmt parser flags.
type LogfmtFlags uint8

// Has whether if flag is enabled.
func (f LogfmtFlags) Has(flag LogfmtFlags) bool {
	return f&flag != 0
}

// Set sets flag.
func (f *LogfmtFlags) Set(flag LogfmtFlags) {
	*f |= flag
}

const (
	// LogfmtFlagStrict whether if parser should stop parsing line
	// if it contains invalid logfmt pairs.
	LogfmtFlagStrict LogfmtFlags = 1 << iota
	// LogfmtFlagKeepEmpty whether if parser should add labels with empty values
	// to the label set.
	LogfmtFlagKeepEmpty
)

// LabelExtractionExpr defines label value to extract.
type LabelExtractionExpr struct {
	Label Label
	Expr  string
}

// RegexpLabelParser extracts labels using regexp capture groups.
type RegexpLabelParser struct {
	Regexp  *regexp.Regexp
	Mapping map[int]Label
}

// PatternLabelParser extracts labels using log pattern.
//
// See https://grafana.com/docs/loki/latest/logql/log_queries/#pattern.
type PatternLabelParser struct {
	Pattern logqlpattern.Pattern
}

// UnpackLabelParser unpacks data from promtail.
//
// See https://grafana.com/docs/loki/latest/logql/log_queries/#unpack.
type UnpackLabelParser struct{}

// LineFormat formats log record using Go template.
type LineFormat struct {
	Template string
}

// DecolorizeExpr decolorizes log line.
type DecolorizeExpr struct{}

// LabelFilter filters records by predicate.
type LabelFilter struct {
	Pred LabelPredicate
}

// LabelPredicate is a label predicate.
type LabelPredicate interface {
	labelPredicate()
	fmt.Stringer
}

// UnparenLabelPredicate recursively extracts [LabelPredicate] from parentheses.
func UnparenLabelPredicate(p LabelPredicate) LabelPredicate {
	for {
		sub, ok := p.(*LabelPredicateParen)
		if !ok {
			return p
		}
		p = sub.X
	}
}

// LabelPredicateBinOp defines a logical operation between predicates.
type LabelPredicateBinOp struct {
	Left  LabelPredicate
	Op    BinOp // OpAnd, OpOr
	Right LabelPredicate
}

// String implements [fmt.Stringer].
func (p *LabelPredicateBinOp) String() string {
	return fmt.Sprintf("%s %s %s", p.Left, p.Op, p.Right)
}

// LabelPredicateParen is a prediacte within parenthesis.
//
// FIXME(tdakkota): are we really need it?
type LabelPredicateParen struct {
	X LabelPredicate
}

// String implements [fmt.Stringer].
func (p *LabelPredicateParen) String() string {
	return "(" + p.X.String() + ")"
}

func (*LabelPredicateBinOp) labelPredicate() {}
func (*LabelPredicateParen) labelPredicate() {}
func (*LabelMatcher) labelPredicate()        {}
func (*DurationFilter) labelPredicate()      {}
func (*BytesFilter) labelPredicate()         {}
func (*NumberFilter) labelPredicate()        {}
func (*IPFilter) labelPredicate()            {}

// IPFilter is a IP filtering predicate (`addr = ip("127.0.0.1")`).
type IPFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq
	Value string
}

// String implements [fmt.Stringer].
func (m IPFilter) String() string {
	return fmt.Sprintf("%s %s ip(%q)", m.Label, m.Op, m.Value)
}

// DurationFilter is a duration filtering predicate (`elapsed > 10s`).
type DurationFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	Value time.Duration
}

// String implements [fmt.Stringer].
func (m DurationFilter) String() string {
	return fmt.Sprintf("%s %s %s", m.Label, labelOpString(m.Op), m.Value)
}

// BytesFilter is a byte size filtering predicate (`size > 10gb`).
type BytesFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	Value uint64
}

// String implements [fmt.Stringer].
func (m BytesFilter) String() string {
	// Remove space between value and size.
	value := strings.ReplaceAll(humanize.IBytes(m.Value), " ", "")
	return fmt.Sprintf("%s %s %s", m.Label, labelOpString(m.Op), value)
}

// NumberFilter is a number filtering predicate (`status >= 400`).
type NumberFilter struct {
	Label Label
	Op    BinOp // OpEq, OpNotEq, OpLt, OpLte, OpGt, OpGte
	// FIXME(tdakkota): add integer field?
	Value float64
}

// String implements [fmt.Stringer].
func (m NumberFilter) String() string {
	return fmt.Sprintf("%s %s %v", m.Label, labelOpString(m.Op), m.Value)
}

func labelOpString(op BinOp) string {
	if op == OpEq {
		return "=="
	}
	return op.String()
}

// LabelFormatExpr renames, modifies or add labels.
type LabelFormatExpr struct {
	// FIXME(tdakkota): use map[K][]V?
	Labels []RenameLabel
	Values []LabelTemplate
}

// LabelTemplate sets value for a label.
type LabelTemplate struct {
	Label    Label
	Template string
}

// RenameLabel renames label.
type RenameLabel struct {
	To   Label
	From Label
}

// DropLabelsExpr drops given labels in a pipeline (i.e. deny list).
type DropLabelsExpr struct {
	Labels   []Label
	Matchers []LabelMatcher
}

// KeepLabelsExpr drops any label except given in a pipeline (i.e. allow list).
type KeepLabelsExpr struct {
	Labels   []Label
	Matchers []LabelMatcher
}

// DistinctFilter filters out lines with duplicate label values.
//
// FIXME(tdakkota): this stage is undocumented.
type DistinctFilter struct {
	Labels []Label
}
