package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// encodeExemplarIDs builds one ExportMetricsServiceRequest whose single gauge point carries one
// exemplar with the given raw trace and span id bytes.
//
// It is hand-encoded rather than marshaled through pdata because pdata cannot express the case
// under test: its TraceID/SpanID are fixed-width arrays, so a marshaled request always carries a
// well-formed id. A malformed one only reaches the decoder from a non-pdata producer.
func encodeExemplarIDs(traceID, spanID []byte) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rm := req.AppendMessage(1) // resource_metrics
	sm := rm.AppendMessage(2)  // scope_metrics
	mt := sm.AppendMessage(2)  // metrics

	mt.AppendString(1, "g")

	dp := mt.AppendMessage(5).AppendMessage(1) // gauge.data_points
	dp.AppendFixed64(3, 1)                     // time_unix_nano
	dp.AppendDouble(4, 1)                      // as_double

	ex := dp.AppendMessage(5) // exemplars
	ex.AppendFixed64(2, 2)    // time_unix_nano
	ex.AppendDouble(3, 0.5)   // as_double

	if spanID != nil {
		ex.AppendBytes(4, spanID)
	}

	if traceID != nil {
		ex.AppendBytes(5, traceID)
	}

	return m.Marshal(nil)
}

// TestConvertMetricsExemplarIDWidth pins that an exemplar id of the wrong width is refused rather
// than stored, matching what pdata does with the same bytes.
func TestConvertMetricsExemplarIDWidth(t *testing.T) {
	t.Parallel()

	var (
		goodTrace = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		goodSpan  = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	)

	for _, tt := range []struct {
		name            string
		traceID, spanID []byte
		wantErr         string
		// wantTraceID and wantSpanID are what a legal exemplar must carry.
		wantTraceID, wantSpanID []byte
	}{
		{
			name:        "well formed",
			traceID:     goodTrace,
			spanID:      goodSpan,
			wantTraceID: goodTrace,
			wantSpanID:  goodSpan,
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
			wantErr: "read exemplar trace id: got 3 bytes, want 16",
		},
		{
			name:    "long trace id",
			traceID: make([]byte, 32),
			wantErr: "read exemplar trace id: got 32 bytes, want 16",
		},
		{
			// Zero bytes do not excuse the width: a 3-byte id is not "no span context".
			name:    "short all-zero trace id",
			traceID: make([]byte, 3),
			wantErr: "read exemplar trace id: got 3 bytes, want 16",
		},
		{
			name:    "long span id",
			traceID: goodTrace,
			spanID:  make([]byte, 9),
			wantErr: "read exemplar span id: got 9 bytes, want 8",
		},
		{
			name:    "short span id",
			spanID:  []byte{1},
			wantErr: "read exemplar span id: got 1 bytes, want 8",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var c otlpdirect.MetricsConverter

			got, dropped, err := c.Convert(encodeExemplarIDs(tt.traceID, tt.spanID))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Zero(t, dropped)

			exs := got.Resources[0].Scopes[0].Metrics[0].Points[0].Exemplars
			require.Len(t, exs, 1)
			assert.Equal(t, tt.wantTraceID, exs[0].TraceID)
			assert.Equal(t, tt.wantSpanID, exs[0].SpanID)
		})
	}
}
