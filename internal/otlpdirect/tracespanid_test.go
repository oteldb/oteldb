package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// spanIDs are the raw id bytes one hand-encoded span and its link carry.
type spanIDs struct {
	trace, span, parent []byte
	linkTrace, linkSpan []byte
}

// encodeSpanIDs builds one ExportTraceServiceRequest whose single span — and that span's single
// link — carry the given raw id bytes.
//
// It is hand-encoded rather than marshaled through pdata because pdata cannot express the case
// under test: its TraceID/SpanID are fixed-width arrays, so a marshaled request always carries a
// well-formed id. A malformed one only reaches the decoder from a non-pdata producer.
func encodeSpanIDs(ids spanIDs) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()
	rs := req.AppendMessage(1) // resource_spans
	ss := rs.AppendMessage(2)  // scope_spans
	sp := ss.AppendMessage(2)  // spans

	appendID := func(mm *easyproto.MessageMarshaler, num uint32, id []byte) {
		if id != nil {
			mm.AppendBytes(num, id)
		}
	}

	appendID(sp, 1, ids.trace)  // trace_id
	appendID(sp, 2, ids.span)   // span_id
	appendID(sp, 4, ids.parent) // parent_span_id

	sp.AppendString(5, "op")   // name
	sp.AppendFixed64(7, 1)     // start_time_unix_nano
	sp.AppendFixed64(8, 2)     // end_time_unix_nano
	ln := sp.AppendMessage(13) // links

	appendID(ln, 1, ids.linkTrace)
	appendID(ln, 2, ids.linkSpan)

	return m.Marshal(nil)
}

// TestConvertTracesIDWidth pins that a span or link id of the wrong width is refused rather than
// stored, matching what pdata does with the same bytes.
func TestConvertTracesIDWidth(t *testing.T) {
	t.Parallel()

	var (
		goodTrace = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		goodSpan  = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	)

	for _, tt := range []struct {
		name    string
		ids     spanIDs
		wantErr string
	}{
		{
			name: "well formed",
			ids: spanIDs{
				trace:     goodTrace,
				span:      goodSpan,
				parent:    goodSpan,
				linkTrace: goodTrace,
				linkSpan:  goodSpan,
			},
		},
		{
			name: "absent",
		},
		{
			name: "empty ids",
			ids: spanIDs{
				trace:     []byte{},
				span:      []byte{},
				parent:    []byte{},
				linkTrace: []byte{},
				linkSpan:  []byte{},
			},
		},
		{
			// An all-zero id of the right width is how OTLP spells "no span context".
			name: "all-zero ids of the right width",
			ids: spanIDs{
				trace:     make([]byte, 16),
				span:      make([]byte, 8),
				parent:    make([]byte, 8),
				linkTrace: make([]byte, 16),
				linkSpan:  make([]byte, 8),
			},
		},
		{
			name:    "short span trace id",
			ids:     spanIDs{trace: []byte{1, 2, 3}},
			wantErr: "read span trace id: got 3 bytes, want 16",
		},
		{
			name:    "long span trace id",
			ids:     spanIDs{trace: make([]byte, 32)},
			wantErr: "read span trace id: got 32 bytes, want 16",
		},
		{
			// Zero bytes do not excuse the width: a 3-byte id is not "no span context".
			name:    "short all-zero span trace id",
			ids:     spanIDs{trace: make([]byte, 3)},
			wantErr: "read span trace id: got 3 bytes, want 16",
		},
		{
			name:    "short span id",
			ids:     spanIDs{trace: goodTrace, span: []byte{1}},
			wantErr: "read span id: got 1 bytes, want 8",
		},
		{
			name:    "long span id",
			ids:     spanIDs{span: make([]byte, 9)},
			wantErr: "read span id: got 9 bytes, want 8",
		},
		{
			name:    "short all-zero span id",
			ids:     spanIDs{span: make([]byte, 7)},
			wantErr: "read span id: got 7 bytes, want 8",
		},
		{
			name:    "short parent span id",
			ids:     spanIDs{span: goodSpan, parent: []byte{1, 2}},
			wantErr: "read parent span id: got 2 bytes, want 8",
		},
		{
			name:    "long parent span id",
			ids:     spanIDs{parent: make([]byte, 16)},
			wantErr: "read parent span id: got 16 bytes, want 8",
		},
		{
			name:    "short all-zero parent span id",
			ids:     spanIDs{parent: make([]byte, 4)},
			wantErr: "read parent span id: got 4 bytes, want 8",
		},
		{
			name:    "short link trace id",
			ids:     spanIDs{trace: goodTrace, span: goodSpan, linkTrace: []byte{9}},
			wantErr: "read link trace id: got 1 bytes, want 16",
		},
		{
			name:    "long link trace id",
			ids:     spanIDs{linkTrace: make([]byte, 17)},
			wantErr: "read link trace id: got 17 bytes, want 16",
		},
		{
			name:    "short all-zero link trace id",
			ids:     spanIDs{linkTrace: make([]byte, 8)},
			wantErr: "read link trace id: got 8 bytes, want 16",
		},
		{
			name:    "short link span id",
			ids:     spanIDs{linkTrace: goodTrace, linkSpan: []byte{9, 9}},
			wantErr: "read link span id: got 2 bytes, want 8",
		},
		{
			name:    "long link span id",
			ids:     spanIDs{linkSpan: make([]byte, 16)},
			wantErr: "read link span id: got 16 bytes, want 8",
		},
		{
			name:    "short all-zero link span id",
			ids:     spanIDs{linkSpan: make([]byte, 1)},
			wantErr: "read link span id: got 1 bytes, want 8",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var c otlpdirect.TracesConverter

			got, err := c.Convert(encodeSpanIDs(tt.ids))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			spans := got.Resources[0].Scopes[0].Spans
			require.Len(t, spans, 1)
			assert.Equal(t, tt.ids.trace, spans[0].TraceID)
			assert.Equal(t, tt.ids.span, spans[0].SpanID)
			assert.Equal(t, tt.ids.parent, spans[0].ParentSpanID)

			require.Len(t, spans[0].Links, 1)
			assert.Equal(t, tt.ids.linkTrace, spans[0].Links[0].TraceID)
			assert.Equal(t, tt.ids.linkSpan, spans[0].Links[0].SpanID)
		})
	}
}
