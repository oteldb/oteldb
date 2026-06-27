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
	}
	return q, nil
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
