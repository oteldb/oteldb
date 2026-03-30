package chstorage

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/logstorage"
)

var _ logqlengine.Querier = (*Querier)(nil)

// Capabilities implements logqlengine.Querier.
func (q *Querier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	caps.Line.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	return caps
}

// Query creates new [InputNode].
func (q *Querier) Query(ctx context.Context, labels []logql.LabelMatcher) (logqlengine.PipelineNode, error) {
	return &InputNode{
		Sel: LogsSelector{
			Labels: labels,
		},
		q: q,
	}, nil
}

// InputNode rebuilds LogQL pipeline in as Clickhouse query.
type InputNode struct {
	Sel LogsSelector

	q *Querier
}

var _ logqlengine.PipelineNode = (*InputNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *InputNode) Traverse(cb logqlengine.NodeVisitor) error {
	return cb(n)
}

// EvalPipeline implements [logqlengine.PipelineNode].
func (n *InputNode) EvalPipeline(ctx context.Context, params logqlengine.EvalParams) (logqlengine.EntryIterator, error) {
	q := LogsQuery[logqlengine.Entry]{
		Sel:       n.Sel,
		Start:     params.Start,
		End:       params.End,
		Direction: params.Direction,
		Limit:     params.Limit,
		Mapper:    entryMapper,
	}
	return q.Execute(ctx, n.q)
}

func entryMapper(r logstorage.Record) (logqlengine.Entry, error) {
	set := logqlabels.NewLabelSet()
	e := logqlengine.Entry{
		Timestamp: r.Timestamp,
		Line:      r.Body,
		Set:       set,
	}
	set.SetFromRecord(r)
	return e, nil
}

// HoppedSamplingNode is a [logqlengine.MetricNode] that offloads LogQL rate/bytes_rate
// to ClickHouse using hop (sliding) windows.
//
// For steps < 1s or range widths < 1s it falls back to raw sampling combined with
// client-side windowing via [logqlmetric.RangeAggregation].
type HoppedSamplingNode struct {
	Sel            LogsSelector
	Sampling       SamplingOp
	GroupingLabels []logql.Label
	// Expr is the original range-aggregation expression, used both to derive the
	// range width and as the fallback aggregator.
	Expr *logql.RangeAggregationExpr

	// fallback is used when the hop optimisation cannot be applied.
	fallback *SamplingNode
	q        *Querier
}

var _ logqlengine.MetricNode = (*HoppedSamplingNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *HoppedSamplingNode) Traverse(cb logqlengine.NodeVisitor) error {
	return cb(n)
}

// EvalMetric implements [logqlengine.MetricNode].
func (n *HoppedSamplingNode) EvalMetric(ctx context.Context, params logqlengine.MetricParams) (logqlengine.StepIterator, error) {
	rangeWidth := n.Expr.Range.Range
	if params.Step < time.Second || rangeWidth < time.Second {
		return n.evalFallback(ctx, params)
	}

	q := SampleHopQuery{
		Start:          params.Start,
		End:            params.End,
		RangeWidth:     rangeWidth,
		Step:           params.Step,
		Sel:            n.Sel,
		Sampling:       n.Sampling,
		GroupingLabels: n.GroupingLabels,
	}
	steps, err := q.Execute(ctx, n.q)
	if err != nil {
		return nil, err
	}
	return iterators.Slice(steps), nil
}

func (n *HoppedSamplingNode) evalFallback(ctx context.Context, params logqlengine.MetricParams) (logqlengine.StepIterator, error) {
	qrange := n.Expr.Range
	iter, err := n.fallback.EvalSample(ctx, logqlengine.EvalParams{
		Start:     params.Start.Add(-qrange.Range),
		End:       params.End,
		Step:      params.Step,
		Direction: logqlengine.DirectionForward,
		Limit:     -1,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fallback sample eval")
	}
	return logqlmetric.RangeAggregation(iter, n.Expr, params.Start, params.End, params.Step)
}

// SamplingNode is a [logqlengine.SampleNode], which offloads sampling to Clickhouse
type SamplingNode struct {
	Sel            LogsSelector
	Sampling       SamplingOp
	GroupingLabels []logql.Label

	q *Querier
}

var _ logqlengine.SampleNode = (*SamplingNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *SamplingNode) Traverse(cb logqlengine.NodeVisitor) error {
	return cb(n)
}

// EvalSample implements [logqlengine.SampleNode].
func (n *SamplingNode) EvalSample(ctx context.Context, params logqlengine.EvalParams) (logqlengine.SampleIterator, error) {
	q := SampleQuery{
		Start:          params.Start,
		End:            params.End,
		Sel:            n.Sel,
		Sampling:       n.Sampling,
		GroupingLabels: n.GroupingLabels,
	}
	return q.Execute(ctx, n.q)
}
