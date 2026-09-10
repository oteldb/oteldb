package tempohandler

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type failingQuerier struct {
	err error
}

func (q failingQuerier) SelectSpansets(context.Context, traceqlengine.SelectSpansetsParams) (iterators.Iterator[traceqlengine.Trace], error) {
	if q.err != nil {
		return nil, q.err
	}
	return iterators.Slice([]traceqlengine.Trace(nil)), nil
}

// A query the engine rejects is not a server fault: neither a query it cannot parse nor one it does
// not implement yet says anything about the health of the server, and 500 sends the reader to look
// at the one place nothing is wrong.
func TestSearchTraceQLStatusCodes(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		query      string
		querierErr error
		code       int
	}{
		"unsupported intrinsic": {"{nestedSetParent<0}", nil, http.StatusNotImplemented},
		"unsupported intrinsic in conjunction": {
			"{nestedSetParent<0 && true}", nil, http.StatusNotImplemented,
		},
		"unsupported child count": {"{childCount>1}", nil, http.StatusNotImplemented},
		"syntax error":            {"{", nil, http.StatusBadRequest},
		"lexer error":             {`{.foo="bar}`, nil, http.StatusBadRequest},
		"budget exceeded": {
			`{.foo="bar"}`,
			errors.Wrap(readbudget.ErrExceeded, "reserve 1227385730 bytes"),
			http.StatusUnprocessableEntity,
		},
		"fault": {`{.foo="bar"}`, errors.New("disk on fire"), http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			engine := traceqlengine.NewEngine(failingQuerier{err: tt.querierErr}, traceqlengine.Options{})
			api := NewTempoAPI(nil, engine, TempoAPIOptions{})

			_, err := api.Search(context.Background(), tempoapi.SearchParams{
				Q: tempoapi.NewOptString(tt.query),
			})
			got, ok := errors.Into[*tempoapi.ErrorStatusCode](err)
			require.True(t, ok, "expected an API error, got %v", err)
			require.Equal(t, tt.code, got.StatusCode)
		})
	}
}
