package otlpdirect_test

import (
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// profileIDs are the raw id bytes one hand-encoded profile and the dictionary link it shares with
// the rest of the request carry.
type profileIDs struct {
	profile             []byte
	linkTrace, linkSpan []byte
}

// encodeProfileIDs builds one ExportProfilesServiceRequest whose dictionary holds a single link
// and whose single profile carries the given raw id bytes.
//
// It is hand-encoded rather than marshaled through pdata because pdata cannot express the case
// under test: its ProfileID/TraceID/SpanID are fixed-width arrays, so a marshaled request always
// carries a well-formed id. A malformed one only reaches the decoder from a non-pdata producer.
func encodeProfileIDs(ids profileIDs) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()

	appendID := func(mm *easyproto.MessageMarshaler, num uint32, id []byte) {
		if id != nil {
			mm.AppendBytes(num, id)
		}
	}

	dict := req.AppendMessage(2) // dictionary
	lk := dict.AppendMessage(4)  // links

	appendID(lk, 1, ids.linkTrace)
	appendID(lk, 2, ids.linkSpan)

	rp := req.AppendMessage(1) // resource_profiles
	sp := rp.AppendMessage(2)  // scope_profiles
	pr := sp.AppendMessage(2)  // profiles

	pr.AppendFixed64(3, 1) // time_nanos

	appendID(pr, 7, ids.profile) // profile_id

	return m.Marshal(nil)
}

// TestConvertProfilesIDWidth pins that a profile or dictionary link id of the wrong width is
// refused rather than stored, matching what pdata does with the same bytes.
func TestConvertProfilesIDWidth(t *testing.T) {
	t.Parallel()

	var (
		goodTrace = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		goodSpan  = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	)

	for _, tt := range []struct {
		name    string
		ids     profileIDs
		wantErr string
	}{
		{
			name: "well formed",
			ids:  profileIDs{profile: goodTrace, linkTrace: goodTrace, linkSpan: goodSpan},
		},
		{
			name: "absent",
		},
		{
			name: "empty ids",
			ids:  profileIDs{profile: []byte{}, linkTrace: []byte{}, linkSpan: []byte{}},
		},
		{
			// An all-zero id of the right width is how OTLP spells "no span context".
			name: "all-zero ids of the right width",
			ids: profileIDs{
				profile:   make([]byte, 16),
				linkTrace: make([]byte, 16),
				linkSpan:  make([]byte, 8),
			},
		},
		{
			name:    "short profile id",
			ids:     profileIDs{profile: []byte{1, 2, 3}},
			wantErr: "read profile id: got 3 bytes, want 16",
		},
		{
			name:    "long profile id",
			ids:     profileIDs{profile: make([]byte, 32)},
			wantErr: "read profile id: got 32 bytes, want 16",
		},
		{
			// Zero bytes do not excuse the width: a 3-byte id is not an absent profile id.
			name:    "short all-zero profile id",
			ids:     profileIDs{profile: make([]byte, 3)},
			wantErr: "read profile id: got 3 bytes, want 16",
		},
		{
			name:    "short link trace id",
			ids:     profileIDs{linkTrace: []byte{9}},
			wantErr: "read link trace id: got 1 bytes, want 16",
		},
		{
			name:    "long link trace id",
			ids:     profileIDs{linkTrace: make([]byte, 17)},
			wantErr: "read link trace id: got 17 bytes, want 16",
		},
		{
			name:    "short all-zero link trace id",
			ids:     profileIDs{linkTrace: make([]byte, 8)},
			wantErr: "read link trace id: got 8 bytes, want 16",
		},
		{
			name:    "short link span id",
			ids:     profileIDs{linkTrace: goodTrace, linkSpan: []byte{9, 9}},
			wantErr: "read link span id: got 2 bytes, want 8",
		},
		{
			name:    "long link span id",
			ids:     profileIDs{linkSpan: make([]byte, 16)},
			wantErr: "read link span id: got 16 bytes, want 8",
		},
		{
			name:    "short all-zero link span id",
			ids:     profileIDs{linkSpan: make([]byte, 1)},
			wantErr: "read link span id: got 1 bytes, want 8",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var c otlpdirect.ProfilesConverter

			got, err := c.Convert(encodeProfileIDs(tt.ids))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			links := got.Dictionary.Links
			require.Len(t, links, 1)
			assert.Equal(t, tt.ids.linkTrace, links[0].TraceID)
			assert.Equal(t, tt.ids.linkSpan, links[0].SpanID)

			profiles := got.Resources[0].Scopes[0].Profiles
			require.Len(t, profiles, 1)
			assert.Equal(t, tt.ids.profile, profiles[0].ProfileID)
		})
	}
}
