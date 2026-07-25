package logqlengine

import (
	"github.com/oteldb/oteldb/internal/logql"
)

// stageKeepsEveryEntry reports whether a pipeline stage is guaranteed to keep every entry it
// processes, i.e. its [Processor] never returns keep=false.
//
// It fails closed: only the stages explicitly listed as non-dropping return true, so a newly added
// stage is assumed to be able to drop entries until it is classified here. Getting that backwards
// would silently under-report a limited query (see [ProcessorNode.EvalPipeline]).
//
// The listing mirrors buildStage:
//
//   - the parsers (json, logfmt, regexp, pattern, unpack) record a parse failure as the __error__
//     label and return the entry unchanged, so they never drop;
//   - line_format, decolorize, label_format, drop and keep rewrite the line or the label set only,
//     and a template error is likewise recorded as __error__;
//   - line filters, label filters and the distinct filter exist to drop entries.
func stageKeepsEveryEntry(stage logql.PipelineStage) bool {
	switch stage.(type) {
	case *logql.JSONExpressionParser,
		*logql.LogfmtExpressionParser,
		*logql.RegexpLabelParser,
		*logql.PatternLabelParser,
		*logql.UnpackLabelParser,
		*logql.LineFormat,
		*logql.DecolorizeExpr,
		*logql.LabelFormatExpr,
		*logql.DropLabelsExpr,
		*logql.KeepLabelsExpr:
		return true
	case *logql.LineFilter,
		*logql.LabelFilter,
		*logql.DistinctFilter:
		return false
	default:
		// Unknown (or newly added) stage: assume it drops.
		return false
	}
}

// keepsEveryEntry reports whether the node passes through every entry the input node yields: the
// prefilter is a no-op and no pipeline stage can drop. The OTEL adapter rewrites the line and never
// drops, so it does not affect the answer.
func (n *ProcessorNode) keepsEveryEntry() bool {
	if p := n.Prefilter; p != nil && p != Processor(NopProcessor) {
		return false
	}
	for _, stage := range n.Pipeline {
		if !stageKeepsEveryEntry(stage) {
			return false
		}
	}
	return true
}
