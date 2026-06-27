package storagebackend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

// TestLabelValueStringParity locks the invariant that a pushdown matcher's value rendering
// (labelValueString) is byte-for-byte identical to what the LogQL label set exposes for the same
// value (logqlabels.LabelSet built from otelAttrs, read via GetString). If these ever diverge a
// pushed matcher could drop a stream/record the in-memory matchSelector would keep — a silent false
// negative. The composite/bytes cases are the ones a naive AppendText would get wrong.
func TestLabelValueStringParity(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    signal.Value
	}{
		{"string", signal.StringValue([]byte("text value"))},
		{"empty-string", signal.StringValue([]byte(""))},
		{"int", signal.IntValue(200)},
		{"negative-int", signal.IntValue(-7)},
		{"bool", signal.BoolValue(true)},
		{"double", signal.DoubleValue(1.5)},
		{"double-int-like", signal.DoubleValue(200)},
		{"bytes", signal.BytesValue([]byte{0x01, 0x02, 0xff})},
		{"slice", signal.SliceValue(signal.IntValue(1), signal.StringValue([]byte("x")))},
		{"map", signal.MapValue(signal.KeyValue{Key: []byte("k"), Value: signal.StringValue([]byte("v"))})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Build the label set exactly as the query path does.
			attrs := otelAttrs(signal.NewAttributes(signal.KeyValue{Key: []byte("attr"), Value: tt.v}))
			set := logqlabels.NewLabelSet()
			set.SetAttrs(attrs)
			want, ok := set.GetString("attr")
			require.True(t, ok)

			require.Equal(t, want, labelValueString(tt.v), "pushdown rendering must equal the label set projection")
		})
	}
}
