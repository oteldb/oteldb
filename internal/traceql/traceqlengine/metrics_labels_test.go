package traceqlengine

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestMetricsLabelEncoding(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		value traceql.Static
	}{
		{"string", traceql.Static{Type: traceql.TypeString, Str: "1:abc\x00:x"}},
		{"integer", traceql.Static{Type: traceql.TypeInt, Data: 1}},
		{"float", traceql.Static{Type: traceql.TypeNumber, Data: math.Float64bits(1)}},
		{"negative zero", traceql.Static{Type: traceql.TypeNumber, Data: math.Float64bits(math.Copysign(0, -1))}},
		{"missing", traceql.Static{Type: traceql.TypeNil}},
		{"boolean", traceql.Static{Type: traceql.TypeBool, Data: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeMetricsLabel(encodeMetricsLabel(tt.value))
			require.NoError(t, err)
			require.Equal(t, tt.value, got)
		})
	}
	integer := encodeMetricsLabel(traceql.Static{Type: traceql.TypeInt, Data: 1})
	text := encodeMetricsLabel(traceql.Static{Type: traceql.TypeString, Str: "1"})
	require.NotEqual(t, integer, text)
	for _, input := range []string{"", ":", "x:0:y", "1:bad!:x"} {
		_, err := decodeMetricsLabel(input)
		require.Error(t, err)
	}
	missing, err := metricsLabels(labels.FromStrings("span.value", encodeMetricsLabel(traceql.Static{Type: traceql.TypeNil})))
	require.NoError(t, err)
	require.Empty(t, missing)
}

func FuzzMetricsLabels(f *testing.F) {
	f.Add("")
	f.Add("1:abc\x00:x")
	f.Add("with ünicode")
	f.Fuzz(func(t *testing.T, value string) {
		_, _ = decodeMetricsLabel(value)
		want := traceql.Static{Type: traceql.TypeString, Str: value}
		got, err := decodeMetricsLabel(encodeMetricsLabel(want))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}
