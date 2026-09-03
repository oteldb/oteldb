package tempohandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/tempoapi"
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

	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}
