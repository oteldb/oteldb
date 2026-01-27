package logqlengine

import (
	"context"
	"time"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// EvalParams sets evaluation parameters.
type EvalParams struct {
	Start     time.Time
	End       time.Time
	Step      time.Duration
	Direction Direction // forward, backward
	Limit     int       // -1 if query should be unlimited.
}

// IsInstant whether query is instant.
func (p EvalParams) IsInstant() bool {
	return p.Start.Equal(p.End) || p.Step == 0
}

// Direction describe log ordering.
type Direction string

const (
	// DirectionBackward sorts records in descending order.
	DirectionBackward Direction = "backward"
	// DirectionForward sorts records in ascending order.
	DirectionForward Direction = "forward"
)

// String implements [fmt.Stringer].
func (d Direction) String() string {
	return string(d)
}

// Entry represents a log entry.
type Entry struct {
	Timestamp otelstorage.Timestamp
	Line      string
	Set       logqlabels.LabelSet
}

type (
	// EntryIterator represents a LogQL entry log stream.
	EntryIterator = iterators.Iterator[Entry]
	// SampleIterator represents a samples stream.
	SampleIterator = iterators.Iterator[logqlmetric.SampledEntry]
	// StepIterator represents a metric stream.
	StepIterator = logqlmetric.StepIterator
)

// NodeVisitor is a callback to traverse [Node].
type NodeVisitor = func(n Node) error

// Node is a generic node interface.
type Node interface {
	// Traverse calls given callback on child nodes.
	Traverse(cb NodeVisitor) error
}

// VisitNode visits nodes of given type.
func VisitNode[N Node](root Node, cb func(N) error) error {
	return root.Traverse(func(n Node) error {
		match, ok := n.(N)
		if !ok {
			return nil
		}
		return cb(match)
	})
}

// PipelineNode represents a LogQL pipeline node.
type PipelineNode interface {
	Node
	EvalPipeline(ctx context.Context, params EvalParams) (EntryIterator, error)
}

// SampleNode represents a log sampling node.
type SampleNode interface {
	Node
	EvalSample(ctx context.Context, params EvalParams) (SampleIterator, error)
}

// MetricParams defines [MetricNode] parameters.
type MetricParams struct {
	Start time.Time
	End   time.Time
	Step  time.Duration
}

// MetricNode represents a LogQL metric function node.
type MetricNode interface {
	Node
	EvalMetric(ctx context.Context, params MetricParams) (StepIterator, error)
}

// Query is a LogQL query.
type Query interface {
	Eval(ctx context.Context, params EvalParams) (lokiapi.QueryResponseData, error)
}
