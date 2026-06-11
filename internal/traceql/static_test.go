package traceql

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestStaticSet(t *testing.T) {
	var s Static

	s.SetString("foo")
	require.Equal(t, TypeString, s.ValueType())
	require.Equal(t, "foo", s.AsString())

	s.SetInt(-10)
	require.Equal(t, TypeInt, s.ValueType())
	require.Equal(t, int64(-10), s.AsInt())

	s.SetNumber(3.14)
	require.Equal(t, TypeNumber, s.ValueType())
	require.Equal(t, 3.14, s.AsNumber())
	s.SetNumber(math.NaN())
	require.Equal(t, TypeNumber, s.ValueType())
	require.True(t, math.IsNaN(s.AsNumber()))

	s.SetBool(true)
	require.Equal(t, TypeBool, s.ValueType())
	require.Equal(t, true, s.AsBool())

	s.SetNil()
	require.Equal(t, TypeNil, s.ValueType())
	require.True(t, s.IsNil())

	s.SetDuration(time.Second)
	require.Equal(t, TypeDuration, s.ValueType())
	require.Equal(t, time.Second, s.AsDuration())

	s.SetSpanStatus(ptrace.StatusCodeOk)
	require.Equal(t, TypeSpanStatus, s.ValueType())
	require.Equal(t, ptrace.StatusCodeOk, s.AsSpanStatus())

	s.SetSpanKind(ptrace.SpanKindClient)
	require.Equal(t, TypeSpanKind, s.ValueType())
	require.Equal(t, ptrace.SpanKindClient, s.AsSpanKind())
}

func TestStatic_SetOTELValue(t *testing.T) {
	byteValue := pcommon.NewValueBytes()
	byteValue.Bytes().Append('f', 'o', 'o')

	tests := []struct {
		val    pcommon.Value
		want   Static
		wantOk bool
	}{
		{pcommon.NewValueStr("foo"), Static{Type: TypeString, Str: "foo"}, true},
		{pcommon.NewValueInt(10), Static{Type: TypeInt, Data: 10}, true},
		{pcommon.NewValueDouble(3.14), Static{Type: TypeNumber, Data: math.Float64bits(3.14)}, true},
		{pcommon.NewValueBool(true), Static{Type: TypeBool, Data: 1}, true},
		{pcommon.NewValueBool(false), Static{Type: TypeBool, Data: 0}, true},
		{byteValue, Static{Type: TypeString, Str: "foo"}, true},

		{pcommon.NewValueMap(), Static{}, false},
		{pcommon.NewValueSlice(), Static{}, false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var got Static
			ok := got.SetOTELValue(tt.val)
			if !tt.wantOk {
				require.False(t, ok)
				return
			}
			require.True(t, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStatic_Compare(t *testing.T) {
	parseStatic := func(t *testing.T, input string) Static {
		p, err := newParser(input)
		require.NoError(t, err)

		s, err := p.parseStatic()
		require.NoError(t, err)
		return *s
	}
	tests := []struct {
		a, b string
		want int
	}{
		// String.
		{`""`, `""`, 0},
		{`"foo"`, `"foo"`, 0},
		{`"foo"`, `"bar"`, strings.Compare("foo", "bar")},
		{`"bar"`, `"foo"`, strings.Compare("bar", "foo")},
		// Integer.
		{`10`, `9`, +1},
		{`10`, `10`, 0},
		{`10`, `11`, -1},
		// Number.
		{`9.99`, `9.9`, +1},
		{`10.0`, `10.0`, 0},
		{`10.1`, `10.1`, 0},
		{`10.0`, `10.1`, -1},
		// Boolean.
		{`true`, `true`, 0},
		{`false`, `false`, 0},
		{`true`, `false`, +1},
		{`false`, `true`, -1},
		// Nil.
		{`nil`, `nil`, 0},
		// Duration.
		{`15ns`, `10ns`, +1},
		{`10ns`, `10ns`, 0},
		{`1s`, `1s`, 0},
		{`1.5s`, `1.5s`, 0},
		{`10ns`, `15ns`, -1},
		// Status.
		{`ok`, `ok`, 0},
		{`unset`, `unset`, 0},
		{`error`, `error`, 0},
		// Kind.
		{`client`, `client`, 0},
		{`server`, `server`, 0},
		{`producer`, `producer`, 0},
		{`consumer`, `consumer`, 0},
		// Numeric.
		{`10`, `9.0`, +1},
		{`10`, `9.9`, +1},
		{`10`, `10.0`, 0},
		{`10`, `10.1`, -1},
		{`10`, `11.0`, -1},
		{`10`, `9ns`, +1},
		{`10`, `10ns`, 0},
		{`10`, `11ns`, -1},
		// Incomparable.
		{`"foo"`, `10`, -1},
		{`true`, `10`, -1},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			a := parseStatic(t, tt.a)
			b := parseStatic(t, tt.b)
			require.Equal(t, tt.want, a.Compare(b))
		})
	}
}
