package lokihandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/lexer"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

func evalErr(ctx context.Context, err error, msg string) error {
	_, isLexerErr := errors.Into[*lexer.Error](err)
	_, isParseErr := errors.Into[*logql.ParseError](err)
	// [readbudget.ErrExceeded] joins the too-large family: same cause (the query asked for more than
	// it is allowed to hold), same remedy (narrow it), so it takes the same status these already do.
	isTooLarge := errors.Is(err, chstorage.ErrLogsTooManySamples) ||
		errors.Is(err, chstorage.ErrLogsResultTooLarge) ||
		errors.Is(err, readbudget.ErrExceeded)
	if isLexerErr || isParseErr || isTooLarge {
		return &lokiapi.ErrorStatusCode{
			StatusCode: http.StatusBadRequest,
			Response:   lokiapi.Error(appendTrace(ctx, err.Error())),
		}
	}

	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func validationErr(ctx context.Context, err error, msg string) error {
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   lokiapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func executionErr(ctx context.Context, err error, msg string) error {
	if errors.Is(err, readbudget.ErrExceeded) {
		return &lokiapi.ErrorStatusCode{
			StatusCode: http.StatusBadRequest,
			Response: lokiapi.Error(appendTrace(ctx, fmt.Sprintf(
				"%s: query needs more memory than its budget allows; narrow the time range or use a more selective matcher: %s",
				msg, err))),
		}
	}

	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}
