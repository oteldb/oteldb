package tempohandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/lexer"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func validationErr(ctx context.Context, err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func executionErr(ctx context.Context, err error, msg string) error {
	// A read refused for want of budget is the query's fault, not the server's: the data is intact
	// and a narrower window or a more selective matcher answers. Reported as 5xx it sends the reader
	// to check the server, which is the one place the problem is not.
	if errors.Is(err, readbudget.ErrExceeded) {
		return &tempoapi.ErrorStatusCode{
			StatusCode: http.StatusUnprocessableEntity,
			Response: tempoapi.Error(appendTrace(ctx, fmt.Sprintf(
				"%s: query needs more memory than its budget allows; narrow the time range or use a more selective matcher: %s",
				msg, err))),
		}
	}

	// [traceqlengine.Engine.Eval] parses the query itself, so a malformed query comes back as an
	// execution failure. It is the client's text that is wrong, not the server.
	_, isLexerErr := errors.Into[*lexer.Error](err)
	_, isSyntaxErr := errors.Into[*traceql.SyntaxError](err)
	_, isTypeErr := errors.Into[*traceql.TypeError](err)
	if isLexerErr || isSyntaxErr || isTypeErr {
		return queryErr(ctx, http.StatusBadRequest, err, msg)
	}

	// A query the engine does not implement yet is valid TraceQL the server cannot answer.
	if _, ok := errors.Into[*traceqlengine.UnsupportedError](err); ok {
		return queryErr(ctx, http.StatusNotImplemented, err, msg)
	}

	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func queryErr(ctx context.Context, code int, err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: code,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}
