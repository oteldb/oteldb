package logqlengine

import (
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-logfmt/logfmt"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LogfmtExtractor is a Logfmt label extractor.
type LogfmtExtractor struct {
	labels map[string]logql.Label
}

func buildLogfmtExtractor(stage *logql.LogfmtExpressionParser) (Processor, error) {
	if f := stage.Flags; f != 0 {
		return nil, &logqlerrors.UnsupportedError{Msg: "logfmt parser flags are unsupported"}
	}

	e := &LogfmtExtractor{
		labels: make(map[string]logql.Label, len(stage.Exprs)+len(stage.Labels)),
	}
	for _, label := range stage.Labels {
		e.labels[string(label)] = label
	}
	for _, expr := range stage.Exprs {
		key := expr.Expr
		if strings.HasPrefix(key, `"`) {
			unquoted, err := strconv.Unquote(key)
			if err != nil {
				return nil, errors.Wrapf(err, "unquote key %q", key)
			}
			key = unquoted
		}
		if existing, ok := e.labels[key]; ok {
			return nil, errors.Errorf("duplicate label %q extractor: as %q and as %q", key, existing, expr.Label)
		}
		e.labels[key] = expr.Label
	}
	return e, nil
}

// Process implements Processor.
func (e *LogfmtExtractor) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	var err error
	if len(e.labels) == 0 {
		err = e.extractAll(line, set)
	} else {
		err = e.extractSome(line, set)
	}
	if err != nil {
		set.SetError("logfmt parsing error", err)
	}
	return line, true
}

func (e *LogfmtExtractor) extractSome(line string, set logqlabels.LabelSet) error {
	// TODO(tdakkota): re-use decoder somehow.
	d := logfmt.NewDecoder(strings.NewReader(line))

	parsed := map[logql.Label]string{}
	for d.ScanRecord() {
		for d.ScanKeyval() {
			if label, ok := e.labels[string(d.Key())]; ok {
				// TODO(tdakkota): try string interning
				parsed[label] = string(d.Value())
			}
		}
	}
	if err := d.Err(); err != nil {
		return err
	}
	for label, val := range parsed {
		set.Set(label, pcommon.NewValueStr(val))
	}
	return nil
}

func (e *LogfmtExtractor) extractAll(line string, set logqlabels.LabelSet) error {
	// TODO(tdakkota): re-use decoder somehow.
	d := logfmt.NewDecoder(strings.NewReader(line))

	parsed := map[logql.Label]string{}
	for d.ScanRecord() {
		for d.ScanKeyval() {
			// TODO(tdakkota): try string interning
			parsed[logql.Label(d.Key())] = string(d.Value())
		}
	}
	if err := d.Err(); err != nil {
		return err
	}
	for label, val := range parsed {
		set.Set(label, pcommon.NewValueStr(val))
	}
	return nil
}
