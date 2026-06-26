package profilehandler

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

func validationErr(ctx context.Context, err error, msg string) error {
	return &pyroscopeapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   pyroscopeapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func executionErr(ctx context.Context, err error, msg string) error {
	return &pyroscopeapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   pyroscopeapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func badRequest(ctx context.Context, msg string) error {
	return &pyroscopeapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   pyroscopeapi.Error(appendTrace(ctx, msg)),
	}
}

func appendTrace(ctx context.Context, s string) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return s
	}
	return fmt.Sprintf("%s (trace_id=%s, span_id=%s)", s, sc.TraceID(), sc.SpanID())
}
