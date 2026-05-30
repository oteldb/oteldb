package logqlengine

import (
	"bytes"
	"text/template"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// LineFormat is a line formatting Processor.
type LineFormat struct {
	tmpl *template.Template

	// Per-line state.
	ts   otelstorage.Timestamp
	line string
	buf  *bytes.Buffer
}

func buildLineFormat(stage *logql.LineFormat) (Processor, error) {
	lf := &LineFormat{
		buf: getTemplateBuffer(),
	}

	tmpl, err := compileTemplate(
		"line",
		stage.Template,
		lf.currentTimestamp,
		lf.currentLine,
	)
	if err != nil {
		return nil, errors.Wrap(err, "invalid line template")
	}
	lf.tmpl = tmpl

	return lf, nil
}

func (lf *LineFormat) currentLine() string {
	return lf.line
}

func (lf *LineFormat) currentTimestamp() time.Time {
	return lf.ts.AsTime()
}

// Process implements Processor.
func (lf *LineFormat) Process(ts otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	lf.ts = ts
	lf.line = line
	lf.buf.Reset()

	if err := lf.tmpl.Execute(lf.buf, set.AsMap()); err != nil {
		set.SetError("template error", err)
		return line, true
	}
	return lf.buf.String(), true
}
