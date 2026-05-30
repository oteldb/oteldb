package logqlengine

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlpattern"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// PatternExtractor is a Pattern label extractor.
type PatternExtractor struct {
	pattern logqlpattern.Pattern
}

func buildPatternExtractor(stage *logql.PatternLabelParser) (Processor, error) {
	return &PatternExtractor{pattern: stage.Pattern}, nil
}

// Process implements Processor.
func (e *PatternExtractor) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	logqlpattern.Match(e.pattern, line, func(l logql.Label, s string) {
		set.Set(l, pcommon.NewValueStr(s))
	})
	return line, true
}
