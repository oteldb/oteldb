package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"

	"github.com/oteldb/storage/signal"
)

// Every ExportXServiceResponse has the same shape: field 1 is an optional partial_success
// submessage whose field 1 is the rejected count and whose field 2 is a human-readable message.
// The count's name differs per signal (rejected_log_records, rejected_spans, rejected_data_points,
// rejected_profiles) but the number does not, so one encoder serves all four.
const (
	fieldResponsePartialSuccess = 1
	fieldPartialRejected        = 1
	fieldPartialErrorMessage    = 2
)

// encodePartialSuccess builds the response for an export that stored some of what it was sent.
//
// OTLP requires 200 with a partial-success body here, not an error status: the rejected items are
// ones no retry can fix, so a client that resends them would loop forever. The message says which
// signal's unit was rejected, since the field name is all that distinguishes them.
func encodePartialSuccess(sig signal.Signal, rejected int) []byte {
	var m easyproto.Marshaler

	ps := m.MessageMarshaler().AppendMessage(fieldResponsePartialSuccess)
	ps.AppendInt64(fieldPartialRejected, int64(rejected))
	ps.AppendString(fieldPartialErrorMessage, rejectionMessage(sig))

	return m.Marshal(nil)
}

func rejectionMessage(sig signal.Signal) string {
	switch sig {
	case signal.Log:
		return "some log records could not be represented and were dropped"
	case signal.Trace:
		return "some spans could not be represented and were dropped"
	case signal.Profile:
		return "some profiles could not be represented and were dropped"
	default:
		return "some data points carried no value and were dropped"
	}
}
