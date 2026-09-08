package xspan_test

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/xspan"
)

func recorded(t *testing.T, do func(tracer trace.Tracer)) tracetest.SpanStub {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	do(tp.Tracer("test"))

	spans := rec.Ended()
	require.Len(t, spans, 1)
	return tracetest.SpanStubFromReadOnlySpan(spans[0])
}

func TestEnd(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantMsg    string
		wantEvents int
	}{
		{"Nil", nil, codes.Unset, "", 0},
		{"Error", errors.New("boom"), codes.Error, "boom", 1},
		{"Wrapped", errors.Wrap(errors.New("boom"), "eval"), codes.Error, "eval: boom", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := recorded(t, func(tracer trace.Tracer) {
				_, span := tracer.Start(context.Background(), "op")
				xspan.End(span, tt.err)
			})

			require.Equal(t, tt.wantCode, got.Status.Code)
			require.Equal(t, tt.wantMsg, got.Status.Description)
			require.Len(t, got.Events, tt.wantEvents)
		})
	}
}

func TestFailDoesNotEnd(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	_, span := tp.Tracer("test").Start(context.Background(), "op")
	xspan.Fail(span, errors.New("boom"))
	require.Empty(t, rec.Ended())

	span.End()
	require.Len(t, rec.Ended(), 1)
}
