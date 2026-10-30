package main

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestMakeTraces(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name                     string
		count, services, payload int
	}{
		{name: "small", count: 4, services: 1, payload: 0},
		{name: "multi service", count: 256, services: 16, payload: 512},
		{name: "odd payload", count: 8, services: 2, payload: 17},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Unix(1_700_000_000, 0)
			td := makeTraces(16, tt.count, tt.services, tt.payload, now, rand.New(rand.NewPCG(1, 3)))
			raw, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
			require.NoError(t, err)
			decoded := ptraceotlp.NewExportRequest()
			require.NoError(t, decoded.UnmarshalProto(raw))
			require.Equal(t, tt.count, decoded.Traces().SpanCount())
			seen := map[uint64]bool{}
			resources := decoded.Traces().ResourceSpans()
			require.Equal(t, tt.services, resources.Len())
			for i := range resources.Len() {
				spans := resources.At(i).ScopeSpans().At(0).Spans()
				for j := range spans.Len() {
					sp := spans.At(j)
					id := sp.SpanID()
					n := binary.BigEndian.Uint64(id[:]) - 1
					require.False(t, seen[n])
					seen[n] = true
					traceID := sp.TraceID()
					require.Equal(t, n/4+1, binary.BigEndian.Uint64(traceID[8:]))
					parent := sp.ParentSpanID()
					if n%4 == 0 {
						require.Zero(t, binary.BigEndian.Uint64(parent[:]))
					} else {
						require.Equal(t, n-n%4+1, binary.BigEndian.Uint64(parent[:]))
					}
					require.Equal(t, uint64(500*time.Microsecond), uint64(sp.EndTimestamp()-sp.StartTimestamp()))
					value, ok := sp.Attributes().Get("bench.payload")
					require.True(t, ok)
					require.Len(t, value.Str(), tt.payload)
				}
			}
			require.Len(t, seen, tt.count)
		})
	}
}
