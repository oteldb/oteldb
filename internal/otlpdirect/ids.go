package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// OTLP fixed-width id sizes.
const (
	traceIDLen   = 16
	spanIDLen    = 8
	profileIDLen = 16
)

// takeID reads a fixed-width id — a trace, span or profile id — rejecting the request when the
// field is neither empty nor exactly width bytes, as pdata's TraceID/SpanID/ProfileID
// UnmarshalProto do.
//
// This decoder's contract is to be indistinguishable from the pdata path, and the parity tests
// cannot hold it to that here: pdata can only ever emit a well-formed id. Nor is a wrong-length id
// degraded data worth keeping. A trace is looked up by a 16-byte id, so one of any other width
// could never be correlated, and the hex label rendered from it links nowhere.
//
// An empty field stays legal: it is how OTLP spells "no span context", and so does an all-zero id
// of the correct width (see [presentID]).
func takeID(fc *easyproto.FieldContext, width int, what string) ([]byte, error) {
	id, err := takeBytes(fc, what)
	if err != nil {
		return nil, err
	}

	if len(id) != 0 && len(id) != width {
		return nil, errors.Errorf("read %s: got %d bytes, want %d", what, len(id), width)
	}

	return id, nil
}

// presentID drops an id that carries no span context. OTLP spells "no trace" as an all-zero id,
// which pdata reports as empty and pdataconv leaves unset — so the two paths must agree.
func presentID(id []byte) []byte {
	for _, b := range id {
		if b != 0 {
			return id
		}
	}

	return nil
}
