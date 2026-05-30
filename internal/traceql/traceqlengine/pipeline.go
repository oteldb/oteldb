package traceqlengine

import (
	"fmt"
	"time"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"

	"github.com/go-faster/errors"
)

// Spanset is a set of spans.
type Spanset struct {
	TraceID otelstorage.TraceID
	Spans   []tracestorage.Span

	Start           time.Time
	RootSpanName    string
	RootServiceName string
	TraceDuration   time.Duration
}

func (set Spanset) evaluateCtx() EvaluateCtx {
	return EvaluateCtx{
		Set: set,
	}
}

// Processor is a log record processor.
type Processor interface {
	Process(sets []Spanset) ([]Spanset, error)
}

// NopProcessor is a processor that does nothing.
var NopProcessor = &nopProcessor{}

type nopProcessor struct{}

// Process implements Processor.
func (*nopProcessor) Process(sets []Spanset) ([]Spanset, error) {
	return sets, nil
}

// Pipeline is a multi-stage processor.
type Pipeline struct {
	Stages []Processor
}

// BuildPipeline builds a new Pipeline.
func BuildPipeline(stages ...traceql.PipelineStage) (Processor, error) {
	switch len(stages) {
	case 0:
		return NopProcessor, nil
	case 1:
		return buildStage(stages[0])
	default:
		procs := make([]Processor, 0, len(stages))
		for i, stage := range stages {
			p, err := buildStage(stage)
			if err != nil {
				return nil, errors.Wrapf(err, "build stage %d", i)
			}
			procs = append(procs, p)
		}
		return &Pipeline{Stages: procs}, nil
	}
}

func buildStage(stage traceql.PipelineStage) (Processor, error) {
	switch stage := stage.(type) {
	case traceql.SpansetExpr:
		return buildSpansetExpr(stage)
	case *traceql.ScalarFilter:
		return buildScalarFilter(stage)
	default:
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported stage %T", stage)}
	}
}

// Process implements Processor.
func (p *Pipeline) Process(sets []Spanset) (_ []Spanset, err error) {
	for _, s := range p.Stages {
		sets, err = s.Process(sets)
		if err != nil {
			return nil, err
		}
	}
	return sets, nil
}
