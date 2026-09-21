package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// encodeLogRecordIDs builds one ExportLogsServiceRequest whose single record carries the given raw
// trace and span id bytes.
//
// It is hand-encoded rather than marshaled through pdata because pdata cannot express the case
// under test: its TraceID/SpanID are fixed-width arrays, so a marshaled request always carries a
// well-formed id. A malformed one only reaches the decoder from a non-pdata producer.
func encodeLogRecordIDs(traceID, spanID []byte) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rl := req.AppendMessage(1) // resource_logs
	sl := rl.AppendMessage(2)  // scope_logs
	rec := sl.AppendMessage(2) // log_records

	rec.AppendFixed64(1, 1)                       // time_unix_nano
	rec.AppendMessage(5).AppendString(1, "hello") // body

	if traceID != nil {
		rec.AppendBytes(9, traceID)
	}

	if spanID != nil {
		rec.AppendBytes(10, spanID)
	}

	return m.Marshal(nil)
}

// TestConvertLogsIDWidth pins that a log record id of the wrong width is refused rather than
// stored, matching what pdata does with the same bytes.
func TestConvertLogsIDWidth(t *testing.T) {
	t.Parallel()

	var (
		goodTrace = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		goodSpan  = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	)

	for _, tt := range []struct {
		name            string
		traceID, spanID []byte
		wantErr         string
	}{
		{
			name:    "well formed",
			traceID: goodTrace,
			spanID:  goodSpan,
		},
		{
			name: "absent",
		},
		{
			name:    "empty ids",
			traceID: []byte{},
			spanID:  []byte{},
		},
		{
			// An all-zero id of the right width is how OTLP spells "no span context".
			name:    "all-zero ids of the right width",
			traceID: make([]byte, 16),
			spanID:  make([]byte, 8),
		},
		{
			name:    "short trace id",
			traceID: []byte{1, 2, 3},
			wantErr: "read log trace id: got 3 bytes, want 16",
		},
		{
			name:    "long trace id",
			traceID: make([]byte, 32),
			wantErr: "read log trace id: got 32 bytes, want 16",
		},
		{
			// Zero bytes do not excuse the width: a 3-byte id is not "no span context".
			name:    "short all-zero trace id",
			traceID: make([]byte, 3),
			wantErr: "read log trace id: got 3 bytes, want 16",
		},
		{
			name:    "short span id",
			traceID: goodTrace,
			spanID:  []byte{1},
			wantErr: "read log span id: got 1 bytes, want 8",
		},
		{
			name:    "long span id",
			spanID:  make([]byte, 9),
			wantErr: "read log span id: got 9 bytes, want 8",
		},
		{
			name:    "short all-zero span id",
			spanID:  make([]byte, 7),
			wantErr: "read log span id: got 7 bytes, want 8",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var c otlpdirect.LogsConverter

			got, dropped, err := c.Convert(encodeLogRecordIDs(tt.traceID, tt.spanID))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Zero(t, dropped)

			recs := got.Resources[0].Scopes[0].Records
			require.Len(t, recs, 1)
			assert.Equal(t, tt.traceID, recs[0].TraceID)
			assert.Equal(t, tt.spanID, recs[0].SpanID)
		})
	}
}
