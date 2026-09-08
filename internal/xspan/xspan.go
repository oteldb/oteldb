// Package xspan provides helpers for reporting errors on a [trace.Span].
package xspan

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Fail records err on span and marks the span as failed.
//
// [trace.Span.RecordError] only appends an exception event: a span whose status is left unset reads
// as a success, so error-filtered views hide the very span that carries the cause.
func Fail(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// End marks span as failed if err is non-nil, then ends it.
//
// Call it from a deferred closure:
//
//	defer func() { xspan.End(span, rerr) }()
//
// A plain defer evaluates rerr where the defer statement is, while it is still nil, so every span
// would report success.
func End(span trace.Span, err error) {
	Fail(span, err)
	span.End()
}
