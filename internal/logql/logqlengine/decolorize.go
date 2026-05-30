package logqlengine

import (
	"regexp"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Decolorize removes ANSI escape codes from line.
type Decolorize struct{}

func buildDecolorize(*logql.DecolorizeExpr) (Processor, error) {
	return &Decolorize{}, nil
}

// Pattern from https://github.com/acarl005/stripansi.
const ansiPattern = "[\u001B\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))"

var ansiRegex = regexp.MustCompile(ansiPattern)

// Process implements Processor.
func (d *Decolorize) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (string, bool) {
	return ansiRegex.ReplaceAllString(line, ""), true
}
