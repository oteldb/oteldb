package logstorage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// NewRecordFromOTEL creates new Record.
func NewRecordFromOTEL(
	res pcommon.Resource,
	scope pcommon.InstrumentationScope,
	record plog.LogRecord,
) Record {
	return Record{
		ObservedTimestamp: record.ObservedTimestamp(),
		Timestamp:         record.Timestamp(),
		TraceID:           otelstorage.TraceID(record.TraceID()),
		SpanID:            otelstorage.SpanID(record.SpanID()),
		Flags:             record.Flags(),
		SeverityText:      record.SeverityText(),
		SeverityNumber:    record.SeverityNumber(),
		Body:              record.Body().AsString(),
		Attrs:             otelstorage.Attrs(record.Attributes()),
		ResourceAttrs:     otelstorage.Attrs(res.Attributes()),
		ScopeName:         scope.Name(),
		ScopeVersion:      scope.Version(),
		ScopeAttrs:        otelstorage.Attrs(scope.Attributes()),
	}
}
