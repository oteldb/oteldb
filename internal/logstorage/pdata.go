package logstorage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// RecordsToLogs converts decoded records back into an OTLP [plog.Logs] payload, e.g. for
// re-ingesting records read out of storage into another backend. Records are grouped into
// ResourceLogs/ScopeLogs by resource and scope identity, mirroring what a real OTLP
// producer would emit (consumers are expected to flatten per-record data regardless of
// grouping, so this is a cosmetic/efficiency choice, not a correctness one).
func RecordsToLogs(records []Record) plog.Logs {
	ld := plog.NewLogs()

	type scopeKey struct {
		resource     otelstorage.Hash
		scopeName    string
		scopeVersion string
		scopeAttrs   otelstorage.Hash
	}
	scopes := map[scopeKey]plog.ScopeLogs{}

	for _, r := range records {
		key := scopeKey{
			resource:     r.ResourceAttrs.Hash(),
			scopeName:    r.ScopeName,
			scopeVersion: r.ScopeVersion,
			scopeAttrs:   r.ScopeAttrs.Hash(),
		}
		sl, ok := scopes[key]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			r.ResourceAttrs.CopyTo(rl.Resource().Attributes())

			sl = rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(r.ScopeName)
			sl.Scope().SetVersion(r.ScopeVersion)
			r.ScopeAttrs.CopyTo(sl.Scope().Attributes())

			scopes[key] = sl
		}

		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(r.Timestamp)
		lr.SetObservedTimestamp(r.ObservedTimestamp)
		lr.SetTraceID(pcommon.TraceID(r.TraceID))
		lr.SetSpanID(pcommon.SpanID(r.SpanID))
		lr.SetFlags(r.Flags)
		lr.SetSeverityText(r.SeverityText)
		lr.SetSeverityNumber(r.SeverityNumber)
		lr.Body().SetStr(r.Body)
		r.Attrs.CopyTo(lr.Attributes())
	}

	return ld
}
