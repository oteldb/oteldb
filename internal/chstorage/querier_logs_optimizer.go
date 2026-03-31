package chstorage

import (
	"context"
	"slices"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlpattern"
)

// ClickhouseOptimizer replaces LogQL engine execution
// nodes with optimzied Clickhouse queries.
type ClickhouseOptimizer struct{}

var _ logqlengine.Optimizer = (*ClickhouseOptimizer)(nil)

// Name returns optimizer name.
func (o *ClickhouseOptimizer) Name() string {
	return "ClickhouseOptimizer"
}

var nopLogger = zap.NewNop()

// Optimize implements [Optimizer].
func (o *ClickhouseOptimizer) Optimize(ctx context.Context, q logqlengine.Query) (logqlengine.Query, error) {
	lg := nopLogger
	if logqlengine.IsExplainQuery(ctx) {
		lg = zctx.From(ctx).With(
			zap.String("optimizer", o.Name()),
		)
	}

	switch q := q.(type) {
	case *logqlengine.LogQuery:
		q.Root = o.optimizePipeline(q.Root, lg)
	case *logqlengine.MetricQuery:
		if err := logqlengine.VisitNode(q.Root, func(n *logqlengine.SamplingNode) error {
			n.Input = o.optimizePipeline(n.Input, lg)
			return nil
		}); err != nil {
			return nil, err
		}
		q.Root = o.optimizeSampling(q.Root, lg)
	}
	return q, nil
}

func (o *ClickhouseOptimizer) optimizeSampling(n logqlengine.MetricNode, lg *zap.Logger) logqlengine.MetricNode {
	switch n := n.(type) {
	case *logqlengine.VectorAggregation:
		switch n.Expr.Op {
		case logql.VectorOpBottomk, logql.VectorOpTopk,
			logql.VectorOpSort, logql.VectorOpSortDesc:
			return n
		}

		labels, ok := getGroupByLabels(n.Expr.Grouping)
		if !ok {
			return n
		}

		rn, ok := n.Input.(*logqlengine.RangeAggregation)
		if !ok {
			return n
		}
		n.Input = o.buildRangeAggregationSampling(rn, labels, lg)

		return n
	case *logqlengine.LabelReplace:
		n.Input = o.optimizeSampling(n.Input, lg)
		return n
	case *logqlengine.LiteralBinOp:
		n.Input = o.optimizeSampling(n.Input, lg)
		return n
	case *logqlengine.BinOp:
		n.Left = o.optimizeSampling(n.Left, lg)
		n.Right = o.optimizeSampling(n.Right, lg)
		return n
	default:
		return n
	}
}

func (o *ClickhouseOptimizer) buildRangeAggregationSampling(n *logqlengine.RangeAggregation, grouping []logql.Label, lg *zap.Logger) logqlengine.MetricNode {
	if g := n.Expr.Grouping; g != nil {
		return n
	}

	sampleNode, ok := n.Input.(*logqlengine.SamplingNode)
	if !ok {
		return n
	}

	// If it is possible to offload the pipeline to Clickhouse entirely
	// preceding optimizer should replace node with [InputNode].
	pipelineNode, ok := sampleNode.Input.(*InputNode)
	if !ok {
		return n
	}

	op, ok := getSamplingOp(n.Expr)
	if !ok {
		return n
	}

	if ce := lg.Check(zap.DebugLevel, "Sampling could be offloaded to Clickhouse"); ce != nil {
		ce.Write(
			zap.Stringer("sampling_op", op),
			zap.Stringers("grouping_labels", grouping),
		)
	}
	n.Input = &SamplingNode{
		Sel:            pipelineNode.Sel,
		Sampling:       op,
		GroupingLabels: grouping,
		q:              pipelineNode.q,
	}
	return n
}

func getGroupByLabels(g *logql.Grouping) ([]logql.Label, bool) {
	if g == nil || g.Without || len(g.Labels) == 0 {
		return nil, false
	}
	return g.Labels, true
}

func getSamplingOp(e *logql.RangeAggregationExpr) (SamplingOp, bool) {
	if er := e.Range; er.Unwrap != nil || er.Offset != nil {
		return 0, false
	}
	switch e.Op {
	case logql.RangeOpCount, logql.RangeOpRate:
		return CountSampling, true
	case logql.RangeOpBytes, logql.RangeOpBytesRate:
		return BytesSampling, true
	default:
		return 0, false
	}
}

func (o *ClickhouseOptimizer) optimizePipeline(n logqlengine.PipelineNode, lg *zap.Logger) logqlengine.PipelineNode {
	pn, ok := n.(*logqlengine.ProcessorNode)
	if !ok {
		return n
	}

	sn, ok := pn.Input.(*InputNode)
	if !ok {
		// NOTE(tdakkota): this should not happen as long
		// 	as there is only one possible node made by storage.
		return n
	}

	sn.Sel.Line = o.offloadLineFilters(pn.Pipeline)
	if f := sn.Sel.Line; len(f) > 0 {
		if ce := lg.Check(zap.DebugLevel, "Offloading line filters"); ce != nil {
			ce.Write(zap.Stringers("line_filters", f))
		}
	}

	sn.Sel.PipelineLabels = o.offloadLabelFilters(pn.Pipeline)
	if f := sn.Sel.PipelineLabels; len(f) > 0 {
		if ce := lg.Check(zap.DebugLevel, "Offloading pipeline label filters"); ce != nil {
			ce.Write(zap.Stringers("pipeline_labels", f))
		}
	}

	offloaded := len(sn.Sel.Line) + len(sn.Sel.PipelineLabels)
	// Replace original node with [InputNode], since we can execute filtering entirely in
	// Clickhouse.
	if len(pn.Pipeline) == offloaded && !pn.EnableOTELAdapter {
		lg.Debug("Pipeline could be fully offloaded to Clickhouse",
			zap.Stringer("selector", logql.Selector{Matchers: sn.Sel.Labels}),
		)
		return sn
	}
	return n
}

func (o *ClickhouseOptimizer) offloadLabelFilters(pipeline []logql.PipelineStage) (filters []logql.LabelPredicate) {
	labels := make(map[logql.Label]struct{}, 4)
nextStage:
	for i, stage := range pipeline {
		f, ok := stage.(*logql.LabelFilter)
		if !ok {
			continue
		}

		// Collect labels that predicate matches.
		clear(labels)
		if !o.collectPredicateLabels(labels, f.Pred) {
			continue
		}

		// Ensure that any stage before filter does not affect labels
		// that predicates matches.
		for label := range labels {
			if slices.ContainsFunc(pipeline[:i], func(stage logql.PipelineStage) bool {
				return o.affectsLabel(stage, label)
			}) {
				continue nextStage
			}
		}

		filters = append(filters, f.Pred)
	}
	return filters
}

func (o *ClickhouseOptimizer) affectsLabel(stage logql.PipelineStage, label logql.Label) bool {
	isErrorLabel := label == logql.ErrorLabel ||
		label == logql.ErrorDetailsLabel

	inLabelExpr := func(exprs []logql.LabelExtractionExpr, label logql.Label) bool {
		return slices.ContainsFunc(exprs, func(expr logql.LabelExtractionExpr) bool {
			return expr.Label == label
		})
	}

	parserAffectsLabel := func(isErrorLabel bool, labels []logql.Label, exprs []logql.LabelExtractionExpr) bool {
		// Parsing might fail.
		if isErrorLabel {
			return true
		}

		// All parsed fields would be added to label set.
		if len(labels)+len(exprs) == 0 {
			return true
		}

		// Any parsed label is affected.
		return slices.Contains(labels, label) ||
			inLabelExpr(exprs, label)
	}

	switch stage := stage.(type) {
	case *logql.LineFilter:
		return false
	case *logql.JSONExpressionParser:
		return parserAffectsLabel(isErrorLabel, stage.Labels, stage.Exprs)
	case *logql.LogfmtExpressionParser:
		return parserAffectsLabel(isErrorLabel, stage.Labels, stage.Exprs)
	case *logql.RegexpLabelParser:
		for _, v := range stage.Mapping {
			if v == label {
				return true
			}
		}
		return false
	case *logql.PatternLabelParser:
		for _, p := range stage.Pattern.Parts {
			if p.Type == logqlpattern.Capture &&
				p.Value == string(label) {
				return true
			}
		}
		return false
	case *logql.UnpackLabelParser:
		return true
	case *logql.LineFormat:
		return isErrorLabel
	case *logql.DecolorizeExpr:
		return false
	case *logql.LabelFilter:
		return isErrorLabel && o.failableLabelPredicate(stage.Pred)
	case *logql.LabelFormatExpr:
		// Template might fail, occurred error would be added to label set.
		if len(stage.Values) > 0 && isErrorLabel {
			return true
		}

		// Any renamed-to label is affected.
		if slices.ContainsFunc(stage.Labels, func(expr logql.RenameLabel) bool {
			return expr.To == label
		}) {
			return true
		}

		// Any formatted label is affected.
		return slices.ContainsFunc(stage.Values, func(expr logql.LabelTemplate) bool {
			return expr.Label == label
		})
	default:
		return true
	}
}

func (o *ClickhouseOptimizer) collectPredicateLabels(labels map[logql.Label]struct{}, pred logql.LabelPredicate) bool {
	pred = logql.UnparenLabelPredicate(pred)

	switch pred := pred.(type) {
	case *logql.LabelPredicateBinOp:
		switch pred.Op {
		case logql.OpAnd, logql.OpOr:
		default:
			return false
		}
		return o.collectPredicateLabels(labels, pred.Left) &&
			o.collectPredicateLabels(labels, pred.Right)
	case *logql.LabelMatcher:
		labels[pred.Label] = struct{}{}
		return true
	default:
		return false
	}
}

func (o *ClickhouseOptimizer) failableLabelPredicate(pred logql.LabelPredicate) bool {
	pred = logql.UnparenLabelPredicate(pred)

	switch pred := pred.(type) {
	case *logql.LabelPredicateBinOp:
		return o.failableLabelPredicate(pred.Left) ||
			o.failableLabelPredicate(pred.Right)
	case *logql.LabelMatcher:
		return false
	default:
		return true
	}
}

func (o *ClickhouseOptimizer) offloadLineFilters(pipeline []logql.PipelineStage) (line []logql.LineFilter) {
stageLoop:
	for _, stage := range pipeline {
		switch stage := stage.(type) {
		case *logql.LineFilter:
			if !o.canOffloadLineFilter(stage) {
				continue
			}
			line = append(line, *stage)
		case *logql.JSONExpressionParser,
			*logql.LogfmtExpressionParser,
			*logql.RegexpLabelParser,
			*logql.PatternLabelParser,
			*logql.LabelFilter,
			*logql.LabelFormatExpr,
			*logql.DropLabelsExpr,
			*logql.KeepLabelsExpr,
			*logql.DistinctFilter:
			// Do nothing on line, just skip.
		default:
			// Stage modify the line, can't offload line filters after this stage.
			break stageLoop
		}
	}
	return line
}

func (o *ClickhouseOptimizer) canOffloadLineFilter(lf *logql.LineFilter) bool {
	switch lf.Op {
	case logql.OpPattern, logql.OpNotPattern:
		return false
	}
	if lf.By.IP {
		return false
	}
	for _, by := range lf.Or {
		if by.IP {
			return false
		}
	}
	return true
}
