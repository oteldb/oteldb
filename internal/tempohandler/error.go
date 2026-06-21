package tempohandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/oteldb/oteldb/internal/tempoapi"
)

func validationErr(ctx context.Context, err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}

func executionErr(ctx context.Context, err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   tempoapi.Error(appendTrace(ctx, fmt.Sprintf("%s: %s", msg, err))),
	}
}
