package tempohandler

import (
	"context"
	"net/http"

	"github.com/go-faster/jx"
	"github.com/ogen-go/ogen/ogenerrors"
)

// ErrorHandler encodes decoder errors using Tempo's JSON-string error schema.
func ErrorHandler(ctx context.Context, w http.ResponseWriter, _ *http.Request, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ogenerrors.ErrorCode(err))
	e := jx.GetEncoder()
	defer jx.PutEncoder(e)
	e.Str(appendTrace(ctx, err.Error()))
	_, _ = w.Write(e.Bytes())
}
