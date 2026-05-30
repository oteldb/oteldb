package logqlengine

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// UnpackExtractor extracts log entry fron Promtail `pack`-ed entry.
type UnpackExtractor struct{}

func buildUnpackExtractor(*logql.UnpackLabelParser) (Processor, error) {
	return &UnpackExtractor{}, nil
}

// Process implements Processor.
func (e *UnpackExtractor) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (string, bool) {
	newLine, err := parsePackEntry(line, set)
	if err != nil {
		set.SetError("unpack JSON parsing error", err)
		return line, true
	}
	return newLine, true
}

func parsePackEntry(oldLine string, set logqlabels.LabelSet) (string, error) {
	var (
		d    = jx.DecodeStr(oldLine)
		line = oldLine
	)
	if err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		if tt := d.Next(); tt != jx.String {
			// Just ignore non-string fields.
			return d.Skip()
		}

		parsed, err := d.Str()
		if err != nil {
			return errors.Wrapf(err, "parse %q", key)
		}

		if string(key) == "_entry" {
			line = parsed
			return nil
		}

		if err := logql.IsValidLabel(key, set.AllowDots()); err != nil {
			return errors.Wrapf(err, "invalid label %q", key)
		}
		set.Set(logql.Label(key), pcommon.NewValueStr(parsed))

		return nil
	}); err != nil {
		return line, errors.Wrap(err, "parse entry object")
	}
	return line, nil
}
