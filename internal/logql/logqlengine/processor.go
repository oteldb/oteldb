package logqlengine

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Processor is a log record processor.
type Processor interface {
	Process(ts otelstorage.Timestamp, line string, labels logqlabels.LabelSet) (newLine string, keep bool)
}

// NopProcessor is a processor that does nothing.
var NopProcessor = &nopProcessor{}

type nopProcessor struct{}

// Process implements Processor.
func (*nopProcessor) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (string, bool) {
	return line, true
}

// Pipeline is a multi-stage processor.
type Pipeline struct {
	Stages []Processor
}

// BuildPipeline builds a new Pipeline.
func BuildPipeline(stages ...logql.PipelineStage) (Processor, error) {
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

func buildStage(stage logql.PipelineStage) (Processor, error) {
	switch stage := stage.(type) {
	case *logql.LineFilter:
		return buildLineFilter(stage)
	case *logql.JSONExpressionParser:
		return buildJSONExtractor(stage)
	case *logql.LogfmtExpressionParser:
		return buildLogfmtExtractor(stage)
	case *logql.RegexpLabelParser:
		return buildRegexpExtractor(stage)
	case *logql.PatternLabelParser:
		return buildPatternExtractor(stage)
	case *logql.UnpackLabelParser:
		return buildUnpackExtractor(stage)
	case *logql.LineFormat:
		return buildLineFormat(stage)
	case *logql.DecolorizeExpr:
		return buildDecolorize(stage)
	case *logql.LabelFilter:
		return buildLabelFilter(stage)
	case *logql.LabelFormatExpr:
		return buildLabelFormat(stage)
	case *logql.DropLabelsExpr:
		return buildDropLabels(stage)
	case *logql.KeepLabelsExpr:
		return buildKeepLabels(stage)
	case *logql.DistinctFilter:
		return buildDistinctFilter(stage)
	default:
		return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("unsupported stage %T", stage)}
	}
}

// Process implements Processor.
func (p *Pipeline) Process(ts otelstorage.Timestamp, line string, attrs logqlabels.LabelSet) (_ string, keep bool) {
	for _, s := range p.Stages {
		line, keep = s.Process(ts, line, attrs)
		if !keep {
			return
		}
	}
	return line, true
}
