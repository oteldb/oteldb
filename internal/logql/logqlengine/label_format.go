package logqlengine

import (
	"bytes"
	"text/template"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func buildLabelFormat(stage *logql.LabelFormatExpr) (Processor, error) {
	if len(stage.Values) == 0 {
		return &RenameLabel{pairs: stage.Labels}, nil
	}

	format := &LabelFormat{
		rename:  RenameLabel{pairs: stage.Labels},
		formats: make([]labelTemplate, 0, len(stage.Values)),
		buf:     getTemplateBuffer(),
	}
	for _, v := range stage.Values {
		tmpl, err := compileTemplate(
			string(v.Label),
			v.Template,
			format.currentTimestamp,
			format.currentLine,
		)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid label %q template", string(v.Label))
		}
		format.formats = append(format.formats, labelTemplate{
			Label:    v.Label,
			Template: tmpl,
		})
	}
	return format, nil
}

// RenameLabel is a label renaming Processor.
type RenameLabel struct {
	pairs []logql.RenameLabel
}

// Process implements Processor.
func (rl *RenameLabel) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	for _, p := range rl.pairs {
		if v, ok := set.Get(p.From); ok {
			set.Set(p.To, v)
			set.Delete(p.From)
		}
	}
	return line, true
}

type labelTemplate struct {
	Label    logql.Label
	Template *template.Template
}

// LabelFormat is a label formatting Processor.
type LabelFormat struct {
	rename  RenameLabel
	formats []labelTemplate

	// Per-line state.
	ts   otelstorage.Timestamp
	line string
	// Per-label state.
	buf *bytes.Buffer
}

func (lf *LabelFormat) currentLine() string {
	return lf.line
}

func (lf *LabelFormat) currentTimestamp() time.Time {
	return lf.ts.AsTime()
}

// Process implements Processor.
func (lf *LabelFormat) Process(ts otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	line, _ = lf.rename.Process(ts, line, set)

	lf.ts = ts
	lf.line = line
	m := set.AsMap()

	for _, p := range lf.formats {
		lf.buf.Reset()

		if err := p.Template.Execute(lf.buf, m); err != nil {
			set.SetError("template error", err)
			continue
		}

		set.Set(p.Label, pcommon.NewValueStr(lf.buf.String()))
	}
	return line, true
}
