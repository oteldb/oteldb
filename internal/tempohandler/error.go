package tempohandler

import (
	"fmt"
	"net/http"

	"github.com/oteldb/oteldb/internal/tempoapi"
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
