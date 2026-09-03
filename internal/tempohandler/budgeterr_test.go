package tempohandler

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/tempoapi"
)

// A budget refusal means the query asked for more than it may hold, so the data is intact and a
// narrower window answers. Reported as 5xx it reads as a server fault and sends the user to check
// the server, which is the one place nothing is wrong.
func TestExecutionErrSeparatesBudgetFromFault(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		err  error
		code int
	}{
		"budget": {
			errors.Wrap(readbudget.ErrExceeded, "reserve 1227385730 bytes: holding 0 of 966367641"),
			http.StatusUnprocessableEntity,
		},
		// The sentinel arrives wrapped in the peer's address when the read was fanned out, which is
		// the shape a clustered deployment actually produces.
		"budget from a peer": {
			errors.Wrap(errors.Wrap(readbudget.ErrExceeded, `"oteldb-1:7946" fetch`), "select spansets"),
			http.StatusUnprocessableEntity,
		},
		"fault": {errors.New("disk on fire"), http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := errors.Into[*tempoapi.ErrorStatusCode](executionErr(context.Background(), tt.err, "eval"))
			require.True(t, ok)
			require.Equal(t, tt.code, got.StatusCode)

			if tt.code == http.StatusInternalServerError {
				return
			}

			assert.Contains(t, string(got.Response), "narrow the time range",
				"the response says what to do about it")
		})
	}
}
