package logqlengine

import (
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logstorage"
)

// LineFromEntry returns a JSON line from a log record.
func LineFromEntry(entry Entry) string {
	// Create JSON object from record.
	e := &jx.Encoder{}
	e.Obj(func(e *jx.Encoder) {
		if entry.Line != "" {
			e.Field(logstorage.LabelBody, func(e *jx.Encoder) {
				e.Str(entry.Line)
			})
		}
		entry.Set.Range(func(k logql.Label, v pcommon.Value) {
			e.Field(string(k), func(e *jx.Encoder) {
				switch v.Type() {
				case pcommon.ValueTypeStr:
					e.Str(v.Str())
				case pcommon.ValueTypeBool:
					e.Bool(v.Bool())
				case pcommon.ValueTypeInt:
					e.Int64(v.Int())
				case pcommon.ValueTypeDouble:
					e.Float64(v.Double())
				default:
					// Fallback.
					e.Str(v.AsString())
				}
			})
		})
	})
	return e.String()
}
