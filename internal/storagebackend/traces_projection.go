package storagebackend

import (
	"slices"

	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/traceql"
)

func traceProjection(fields []traceql.Attribute) []string {
	if fields == nil {
		return nil
	}
	columns := []string{
		sigtrace.ColTraceID, sigtrace.ColSpanID, sigtrace.ColParentSpanID,
		sigtrace.ColName, sigtrace.ColDuration,
	}
	add := func(column string) {
		if !slices.Contains(columns, column) {
			columns = append(columns, column)
		}
	}
	for _, field := range fields {
		switch field.Prop {
		case traceql.SpanAttribute:
			// Search echoes matching attribute names from spans even when another scope referenced them.
			add(sigtrace.ColAttrs)
			switch field.Scope {
			case traceql.ScopeEvent:
				add(sigtrace.ColEvents)
			case traceql.ScopeLink:
				add(sigtrace.ColLinks)
			}
		case traceql.SpanKind:
			add(sigtrace.ColKind)
		case traceql.SpanStatus:
			add(sigtrace.ColStatusCode)
		case traceql.SpanStatusMessage:
			add(sigtrace.ColStatusMsg)
		case traceql.EventName, traceql.EventTimeSinceStart:
			add(sigtrace.ColEvents)
		case traceql.LinkTraceID, traceql.LinkSpanID:
			add(sigtrace.ColLinks)
		case traceql.SpanDuration, traceql.SpanChildCount, traceql.SpanName, traceql.SpanParent,
			traceql.RootSpanName, traceql.RootServiceName, traceql.TraceDuration,
			traceql.NestedSetLeft, traceql.NestedSetRight, traceql.NestedSetParent,
			traceql.SpanID, traceql.ParentID, traceql.TraceID,
			traceql.InstrumentationName, traceql.InstrumentationVersion:
			// Base columns and stream identity serve these fields and structural evaluation.
		default:
			return nil
		}
	}
	return columns
}
