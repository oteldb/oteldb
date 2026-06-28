package storagebackend

import (
	"bytes"
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// LogQLOptimizer offloads LogQL line filters to the storage fetch layer for the embedded backend.
//
// The leading positive substring filters (`|= "x"`) of a pipeline are pushed into the log fetch as
// body conditions, so the storage drops non-matching records before they are materialized into
// entries with label sets — the expensive part of the scan. The conditions carry an exact contains
// Match, so the result is identical to evaluating the filter in the engine (the engine still applies
// the full pipeline on the surviving entries, so the offload only ever skips work).
//
// It deliberately does not set a bloom token hint: the body bloom is word-tokenized, so a token hint
// would wrongly prune a part that holds a sub-word substring match (e.g. `|= "GET"` matching
// "xGETy"), violating the fetch contract's no-false-negatives guarantee. The offload is therefore a
// precise per-record prefilter, not a part-pruning hint.
//
// Add it to a LogQL engine's optimizers when the querier is a storage [LogQuerier]; it is a no-op
// for any other node type.
type LogQLOptimizer struct{}

var _ logqlengine.Optimizer = (*LogQLOptimizer)(nil)

// Name implements [logqlengine.Optimizer].
func (*LogQLOptimizer) Name() string { return "StorageLogQLOptimizer" }

// Optimize implements [logqlengine.Optimizer].
func (o *LogQLOptimizer) Optimize(_ context.Context, q logqlengine.Query) (logqlengine.Query, error) {
	switch q := q.(type) {
	case *logqlengine.LogQuery:
		o.optimizePipeline(q.Root)
	case *logqlengine.MetricQuery:
		if err := logqlengine.VisitNode(q.Root, func(n *logqlengine.SamplingNode) error {
			o.optimizePipeline(n.Input)
			return nil
		}); err != nil {
			return nil, err
		}
		q.Root = o.optimizeSampling(q.Root)
	}
	return q, nil
}

// optimizeSampling rewrites `sum [by (<level labels>)] (count_over_time|bytes_over_time(<bare
// selector>[w]))` to push the per-(step, level) bucketing into the columnar scan via
// [bucketSamplingNode]. Summing per-line count/bytes grouped by the outer labels equals summing the
// raw samples under those labels, so the offload is exact for `sum`; it is gated to severity-derived
// grouping (or none) because that is what the bucketed node can extract without a full label set.
func (o *LogQLOptimizer) optimizeSampling(n logqlengine.MetricNode) logqlengine.MetricNode {
	switch n := n.(type) {
	case *logqlengine.VectorAggregation:
		if n.Expr.Op != logql.VectorOpSum {
			return n
		}
		by, ok := sumGroupingLabels(n.Expr.Grouping)
		if !ok {
			return n
		}
		if rn, ok := n.Input.(*logqlengine.RangeAggregation); ok {
			o.offloadRangeAggregation(rn, by)
		}
		return n
	case *logqlengine.LabelReplace:
		n.Input = o.optimizeSampling(n.Input)
	case *logqlengine.LiteralBinOp:
		n.Input = o.optimizeSampling(n.Input)
	case *logqlengine.BinOp:
		n.Left = o.optimizeSampling(n.Left)
		n.Right = o.optimizeSampling(n.Right)
	}
	return n
}

// offloadRangeAggregation swaps a range-aggregation's generic sampling node for a bucketSamplingNode
// when the shape is offload-safe: a bare storage selector (no pipeline above), count/bytes sampling
// with no unwrap/offset, and no inner grouping (the outer sum's grouping is carried in by).
func (o *LogQLOptimizer) offloadRangeAggregation(rn *logqlengine.RangeAggregation, by []logql.Label) {
	if rn.Expr.Grouping != nil {
		return
	}
	op, ok := samplingOpFor(rn.Expr)
	if !ok {
		return
	}
	sn, ok := rn.Input.(*logqlengine.SamplingNode)
	if !ok {
		return
	}
	src := storageNode(sn.Input)
	if src == nil {
		return
	}
	rn.Input = &bucketSamplingNode{inner: sn, src: src, op: op, by: by}
}

// storageNode unwraps a sampling node's input to the underlying storage log node when nothing between
// them filters or transforms records: the bare node, or an empty (no-op) ProcessorNode around it.
// Returns nil if a pipeline stage sits in between (then the offload is unsafe and we fall back).
func storageNode(n logqlengine.PipelineNode) *logStreamNode {
	switch n := n.(type) {
	case *logStreamNode:
		return n
	case *logqlengine.ProcessorNode:
		if len(n.Pipeline) == 0 {
			if sn, ok := n.Input.(*logStreamNode); ok {
				return sn
			}
		}
	}
	return nil
}

// sumGroupingLabels returns the grouping labels of a `sum [by (...)]` when the bucketed path can
// honor them: no grouping (sum over all), or a `by(...)` over only severity-derived labels.
func sumGroupingLabels(g *logql.Grouping) ([]logql.Label, bool) {
	if g == nil {
		return nil, true
	}
	if g.Without || len(g.Labels) == 0 {
		return nil, false
	}
	for _, l := range g.Labels {
		if !isLevelLabel(l) {
			return nil, false
		}
	}
	return g.Labels, true
}

// samplingOpFor maps an offloadable range-aggregation to its per-line sample, or ok=false otherwise.
func samplingOpFor(e *logql.RangeAggregationExpr) (sampleOp, bool) {
	if e.Range.Unwrap != nil || e.Range.Offset != nil {
		return 0, false
	}
	switch e.Op {
	case logql.RangeOpCount, logql.RangeOpRate:
		return countSampling, true
	case logql.RangeOpBytes, logql.RangeOpBytesRate:
		return bytesSampling, true
	default:
		return 0, false
	}
}

// optimizePipeline attaches offloadable line-filter conditions to the storage log node wrapped by a
// ProcessorNode, if any.
func (o *LogQLOptimizer) optimizePipeline(n logqlengine.PipelineNode) {
	pn, ok := n.(*logqlengine.ProcessorNode)
	if !ok {
		return
	}
	sn, ok := pn.Input.(*logStreamNode)
	if !ok {
		return
	}
	sn.conditions = lineFilterConditions(pn.Pipeline)
	sn.pipelineLabels = labelFilterMatchers(pn.Pipeline)
}

// labelFilterMatchers extracts the equality label filters (`| L="v"`) that can be resolved against
// the stored label set: those appearing before any stage that adds, renames, or drops labels (after
// which a filter could reference a derived label rather than a stored one). Only simple equality
// predicates on non-empty values are returned; streamFilters then decides per label whether the
// offload is sound (clean name; resource → Matcher, record → Condition). The filters stay in the
// engine pipeline, so this only ever skips work.
func labelFilterMatchers(pipeline []logql.PipelineStage) (out []logql.LabelMatcher) {
	for _, stage := range pipeline {
		switch stage := stage.(type) {
		case *logql.LabelFilter:
			if m, ok := equalityLabelMatcher(stage.Pred); ok {
				out = append(out, m)
			}
		case *logql.LineFilter, *logql.LineFormat, *logql.DecolorizeExpr:
			// These do not change the stored label set; keep scanning.
		default:
			// A parser, label_format, drop/keep, distinct, or unknown stage may change labels, so a
			// later filter could reference a derived label. Stop offloading here.
			return out
		}
	}
	return out
}

// equalityLabelMatcher returns the underlying matcher of a simple `label = "value"` predicate.
func equalityLabelMatcher(pred logql.LabelPredicate) (logql.LabelMatcher, bool) {
	m, ok := logql.UnparenLabelPredicate(pred).(*logql.LabelMatcher)
	if !ok || m.Op != logql.OpEq || m.Value == "" {
		return logql.LabelMatcher{}, false
	}
	return *m, true
}

// lineFilterConditions builds storage fetch conditions from the leading offloadable line filters of
// a pipeline. It stops at the first stage that may rewrite the line, after which earlier-derived
// conditions no longer hold for the modified line.
func lineFilterConditions(pipeline []logql.PipelineStage) (conds []fetch.Condition) {
stageLoop:
	for _, stage := range pipeline {
		switch stage := stage.(type) {
		case *logql.LineFilter:
			c, ok := lineFilterCondition(stage)
			if !ok {
				continue
			}
			conds = append(conds, c)
		case *logql.JSONExpressionParser,
			*logql.LogfmtExpressionParser,
			*logql.RegexpLabelParser,
			*logql.PatternLabelParser,
			*logql.LabelFilter,
			*logql.LabelFormatExpr,
			*logql.DropLabelsExpr,
			*logql.KeepLabelsExpr,
			*logql.DistinctFilter:
			// These stages parse or filter labels but do not rewrite the line; keep scanning.
		default:
			// A stage that may rewrite the line (e.g. line_format): later filters see a
			// different line, so stop offloading here.
			break stageLoop
		}
	}
	return conds
}

// lineFilterCondition lowers a positive substring line filter (`|= "x"`) to a body fetch condition
// with an exact contains Match. Only this shape is offloaded — negated, regexp, IP, and multi-value
// (`or`) filters are left to the engine.
func lineFilterCondition(lf *logql.LineFilter) (fetch.Condition, bool) {
	if lf.Op != logql.OpEq || len(lf.Or) > 0 || lf.By.IP || lf.By.Re != nil {
		return fetch.Condition{}, false
	}
	needle := []byte(lf.By.Value)
	if len(needle) == 0 {
		return fetch.Condition{}, false
	}
	return fetch.Condition{
		Column: siglog.ColBody,
		Match:  func(v signal.Value) bool { return bytes.Contains(valueBytes(v), needle) },
	}, true
}

// valueBytes returns the raw bytes of a string/bytes column value.
func valueBytes(v signal.Value) []byte {
	if b := v.Str(); b != nil {
		return b
	}
	return v.Bytes()
}
