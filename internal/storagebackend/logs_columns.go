package storagebackend

import (
	"github.com/oteldb/storage/query/fetch"
	siglog "github.com/oteldb/storage/signal/log"
)

func isFixedLogColumn(name string) bool {
	switch name {
	case fetch.TimestampColumn,
		siglog.ColObserved, siglog.ColSeverity, siglog.ColFlags, siglog.ColDropped,
		siglog.ColSeverityText, siglog.ColBody, siglog.ColTraceID, siglog.ColSpanID, siglog.ColAttrs:
		return true
	default:
		return false
	}
}
