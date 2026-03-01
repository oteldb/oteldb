package tempohandler

import (
	"fmt"
	"net/http"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

func validationErr(err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   tempoapi.Error(fmt.Sprintf("%s: %s", msg, err)),
	}
}

func executionErr(err error, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   tempoapi.Error(fmt.Sprintf("%s: %s", msg, err)),
	}
}
