package lokihandler

import (
	"fmt"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/lexer"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

func evalErr(err error, msg string) error {
	_, isLexerErr := errors.Into[*lexer.Error](err)
	_, isParseErr := errors.Into[*logql.ParseError](err)
	isTooLarge := errors.Is(err, chstorage.ErrLogsTooManySamples) ||
		errors.Is(err, chstorage.ErrLogsResultTooLarge)
	if isLexerErr || isParseErr || isTooLarge {
		return &lokiapi.ErrorStatusCode{
			StatusCode: http.StatusBadRequest,
			Response:   lokiapi.Error(err.Error()),
		}
	}

	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(fmt.Sprintf("%s: %s", msg, err)),
	}
}

func validationErr(err error, msg string) error {
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   lokiapi.Error(fmt.Sprintf("%s: %s", msg, err)),
	}
}

func executionErr(err error, msg string) error {
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(fmt.Sprintf("%s: %s", msg, err)),
	}
}
