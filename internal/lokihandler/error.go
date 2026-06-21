package lokihandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/lexer"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

func evalErr(ctx context.Context, err error, msg string) error {
	_, isLexerErr := errors.Into[*lexer.Error](err)
	_, isParseErr := errors.Into[*logql.ParseError](err)
	isTooLarge := errors.Is(err, chstorage.ErrLogsTooManySamples) ||
		errors.Is(err, chstorage.ErrLogsResultTooLarge)
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
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}
