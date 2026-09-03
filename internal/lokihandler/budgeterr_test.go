package lokihandler

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

// Logs go through the same record engines and the same budget as traces, and this handler already
// treats the chstorage too-large errors as the client's problem. A budget refusal is the same kind
// of thing and takes the same status.
func TestLokiBudgetErrorsAreClientErrors(t *testing.T) {
	t.Parallel()

	budget := errors.Wrap(readbudget.ErrExceeded, "reserve 1227385730 bytes: holding 0 of 966367641")

	for name, err := range map[string]error{
		"evalErr":      evalErr(context.Background(), budget, "eval"),
		"executionErr": executionErr(context.Background(), budget, "execute"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := errors.Into[*lokiapi.ErrorStatusCode](err)
			require.True(t, ok)
			assert.Equal(t, http.StatusBadRequest, got.StatusCode)
		})
	}
}

func TestLokiFaultsStayServerErrors(t *testing.T) {
	t.Parallel()

	got, ok := errors.Into[*lokiapi.ErrorStatusCode](executionErr(context.Background(), errors.New("disk on fire"), "execute"))
	require.True(t, ok)
	assert.Equal(t, http.StatusInternalServerError, got.StatusCode)
}
