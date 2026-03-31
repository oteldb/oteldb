package chstorage

import (
	"context"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
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
